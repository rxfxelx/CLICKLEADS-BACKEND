# server.py — Smart Leads API (v2.3.1)
# - Usa token da INSTÂNCIA da UAZAPI
# - Verificação em lotes (chunk) e números só com dígitos (5511...)
# - Coletor síncrono rodando em thread (to_thread.run_sync)
# - SSE com progress/item/done e flag verify_failed

import os, json, re
from typing import AsyncGenerator, List, Tuple, Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx
from anyio import to_thread

from collector import init_state, collect_batch  # coletor síncrono

# --------- ENV ---------
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

# --------- APP / CORS ---------
app = FastAPI(title="Smart Leads API", version="2.3.1")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# --------- UAZAPI bulk (chunks + só dígitos) ---------
async def bulk_check_whatsapp(numbers: List[str]) -> Tuple[List[Optional[bool]], bool]:
    """
    Retorna (flags, failed):
      flags  -> lista de True/False/None por número
      failed -> True se tudo veio None (falha na verificação)
    """
    if not numbers:
        return [], False
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return [None] * len(numbers), True

    # normaliza para SOMENTE DÍGITOS
    nums = [re.sub(r"\D", "", n or "") for n in numbers]

    out_flags: List[Optional[bool]] = [None] * len(nums)
    failed_all = True
    CHUNK = 90

    async with httpx.AsyncClient(timeout=30) as cx:
        for i in range(0, len(nums), CHUNK):
            chunk = nums[i:i + CHUNK]

            headers_list = [
                {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
                {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
                {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
            ]
            bodies = [
                {"numbers": chunk},
                {"data": [{"number": n} for n in chunk]},
            ]

            flags_chunk: List[Optional[bool]] = [None] * len(chunk)
            got_something = False

            for hdr in headers_list:
                for body in bodies:
                    try:
                        r = await cx.post(UAZAPI_CHECK_URL, json=body, headers=hdr)
                        if 200 <= r.status_code < 300:
                            data = r.json()
                            src = data if isinstance(data, list) else (data.get("data") or data.get("numbers") or [])
                            for idx, item in enumerate(src[:len(chunk)]):
                                ok = None
                                if isinstance(item, dict):
                                    ok = item.get("isInWhatsapp")
                                    if ok is None: ok = item.get("is_whatsapp")
                                    if ok is None: ok = item.get("valid") or item.get("exists")
                                flags_chunk[idx] = (bool(ok) if ok is not None else None)
                            got_something = any(v is not None for v in flags_chunk)
                            if got_something:
                                break
                    except Exception:
                        continue
                if got_something:
                    break

            out_flags[i:i + CHUNK] = flags_chunk
            if any(v is not None for v in flags_chunk):
                failed_all = False

    return out_flags, failed_all

# --------- Coleta até atingir meta (sem travar event loop) ---------
async def collect_until_target(
    nicho: str, local: str, n: int, verify: int
) -> Tuple[List[str], int, int, bool, bool]:
    """
    Retorna: (final_list, searched_total, non_wa_count, exhausted_all, verify_failed)
    """
    state = init_state(local)
    pool: List[str] = []
    seen = set()
    exhausted_all = False
    non_wa_count = 0
    verify_failed = False

    while True:
        # roda o coletor síncrono em thread separada
        batch, state, exhausted = await to_thread.run_sync(collect_batch, nicho, state, max(n, 10))
        exhausted_all = exhausted_all or exhausted

        for t in batch:
            if t not in seen:
                seen.add(t)
                pool.append(t)

        searched = len(pool)

        if verify == 0:
            if searched >= n or exhausted_all:
                return pool[:n], searched, 0, exhausted_all, False
        else:
            if searched >= n or exhausted_all:
                flags, failed = await bulk_check_whatsapp(pool)
                verify_failed = failed

                wa = [p for p, ok in zip(pool, flags) if ok is True]
                non_wa_count = sum(1 for ok in flags if ok is False)

                if len(wa) >= n or exhausted_all:
                    return wa[:n], searched, non_wa_count, exhausted_all, verify_failed

        if exhausted_all:
            # acabou a fonte e não atingiu a meta
            return (pool[:n] if verify == 0 else []), searched, non_wa_count, exhausted_all, verify_failed

# --------- Endpoints ---------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        final_list, searched_total, non_wa_count, exhausted, verify_failed = await collect_until_target(
            nicho, local, n, verify
        )
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
        "verify_failed": verify_failed,
    }

@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    async def gen() -> AsyncGenerator[bytes, None]:
        yield b"event: start\ndata: {}\n\n"
        try:
            final_list, searched_total, non_wa_count, exhausted, verify_failed = await collect_until_target(
                nicho, local, n, verify
            )
            wa_count = len(final_list) if verify == 1 else 0

            prog = {"searched": searched_total, "wa_count": wa_count, "non_wa_count": non_wa_count}
            yield f"event: progress\ndata: {json.dumps(prog, ensure_ascii=False)}\n\n".encode()

            for p in final_list:
                payload = {"phone": p}
                if verify == 1:
                    payload["has_whatsapp"] = True
                yield f"event: item\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()

            done = {
                "count": len(final_list),
                "searched": searched_total,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "exhausted": exhausted,
                "verify_failed": verify_failed,
            }
            yield f"event: done\ndata: {json.dumps(done, ensure_ascii=False)}\n\n".encode()
        except Exception as e:
            err = {"error": f"{type(e).__name__}: {e}"}
            yield f"event: error\ndata: {json.dumps(err, ensure_ascii=False)}\n\n".encode()

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
    }
    return StreamingResponse(gen(), headers=headers)
