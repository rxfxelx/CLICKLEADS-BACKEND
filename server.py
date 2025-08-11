# server.py — Smart Leads API (fix NameError + UAZAPI instance token)
import os, json
from typing import AsyncGenerator, List, Tuple
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx

from collector import init_state, collect_batch  # <- só estes

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

app = FastAPI(title="Smart Leads API", version="2.2.1")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- UAZAPI bulk ----------
async def bulk_check_whatsapp(numbers: List[str]) -> List[bool | None]:
    if not numbers:
        return []
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return [None] * len(numbers)

    headers_list = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
        {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
    ]
    bodies = [
        {"numbers": numbers},
        {"data": [{"number": n} for n in numbers]},
    ]

    async with httpx.AsyncClient(timeout=30) as cx:
        for hdr in headers_list:
            for body in bodies:
                try:
                    r = await cx.post(UAZAPI_CHECK_URL, json=body, headers=hdr)
                    if 200 <= r.status_code < 300:
                        data = r.json()
                        out: List[bool | None] = []
                        src = data if isinstance(data, list) else (data.get("data") or data.get("numbers") or [])
                        for item in src:
                            if isinstance(item, dict):
                                ok = item.get("isInWhatsapp")
                                if ok is None: ok = item.get("is_whatsapp")
                                if ok is None: ok = item.get("valid") or item.get("exists")
                                out.append(bool(ok) if ok is not None else None)
                        if out:
                            if len(out) < len(numbers):
                                out.extend([None] * (len(numbers) - len(out)))
                            return out
                except Exception:
                    continue
    return [None] * len(numbers)

# ---------- Coleta até atingir meta ----------
async def collect_until_target(nicho: str, local: str, n: int, verify: int) -> Tuple[List[str], int, int, bool]:
    state = init_state(local)
    pool: List[str] = []
    seen = set()
    exhausted_all = False
    non_wa_count = 0

    while True:
        batch, state, exhausted = collect_batch(nicho, state, max(n, 10))
        exhausted_all = exhausted_all or exhausted
        for t in batch:
            if t not in seen:
                seen.add(t); pool.append(t)

        searched = len(pool)

        if verify == 0:
            if searched >= n or exhausted_all:
                return pool[:n], searched, 0, exhausted_all
        else:
            if searched >= n or exhausted_all:
                flags = await bulk_check_whatsapp(pool)
                wa = [p for p, ok in zip(pool, flags) if ok is True]
                non_wa_count = sum(1 for ok in flags if ok is False)
                if len(wa) >= n or exhausted_all:
                    return wa[:n], searched, non_wa_count, exhausted_all

        if exhausted_all:
            return (pool[:n] if verify == 0 else []), searched, non_wa_count, exhausted_all

# ---------- Endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
async def leads(nicho: str = Query(...), local: str = Query(...),
                n: int = Query(50, ge=1, le=500), verify: int = Query(0, ge=0, le=1)):
    try:
        final_list, searched_total, non_wa_count, exhausted = await collect_until_target(nicho, local, n, verify)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}: {e}")

    if verify == 1:
        items = [{"phone": p, "has_whatsapp": True} for p in final_list]
        wa_count = len(items)
    else:
        items = [{"phone": p} for p in final_list]
        wa_count = 0

    return {
        "count": len(items),
        "items": items,
        "searched": searched_total,
        "wa_count": wa_count,
        "non_wa_count": non_wa_count,
        "exhausted": exhausted,
    }

@app.get("/leads/stream")
async def leads_stream(nicho: str = Query(...), local: str = Query(...),
                       n: int = Query(50, ge=1, le=500), verify: int = Query(0, ge=0, le=1)):
    async def gen() -> AsyncGenerator[bytes, None]:
        yield b"event: start\ndata: {}\n\n"
        try:
            final_list, searched_total, non_wa_count, exhausted = await collect_until_target(nicho, local, n, verify)
            wa_count = len(final_list) if verify == 1 else 0
            prog = {"searched": searched_total, "wa_count": wa_count, "non_wa_count": non_wa_count}
            yield f"event: progress\ndata: {json.dumps(prog, ensure_ascii=False)}\n\n".encode()

            if verify == 1:
                for p in final_list:
                    yield f"event: item\ndata: {json.dumps({'phone': p, 'has_whatsapp': True}, ensure_ascii=False)}\n\n".encode()
            else:
                for p in final_list:
                    yield f"event: item\ndata: {json.dumps({'phone': p}, ensure_ascii=False)}\n\n".encode()

            done = {"count": len(final_list), "searched": searched_total, "wa_count": wa_count,
                    "non_wa_count": non_wa_count, "exhausted": exhausted}
            yield f"event: done\ndata: {json.dumps(done, ensure_ascii=False)}\n\n".encode()
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'error': f'{type(e).__name__}: {e}'}, ensure_ascii=False)}\n\n".encode()

    headers = {"Cache-Control": "no-cache", "Content-Type": "text/event-stream",
               "Connection": "keep-alive", "Access-Control-Allow-Origin": "*"}
    return StreamingResponse(gen(), headers=headers)
