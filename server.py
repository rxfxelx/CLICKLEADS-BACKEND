import os, json, asyncio
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from collector import collect_numbers_info, iter_numbers

# Env para verificação WhatsApp (opcional)
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_ADMIN_TOKEN = os.getenv("UAZAPI_ADMIN_TOKEN")

app = FastAPI(title="Smart Leads API", version="1.7.0")

# CORS (libere conforme seu domínio de front)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# ---------- WhatsApp check ----------
async def check_whatsapp(e164: str) -> bool | None:
    if not (UAZAPI_CHECK_URL and UAZAPI_ADMIN_TOKEN):
        return None
    headers_try = [
        {"Authorization": f"Bearer {UAZAPI_ADMIN_TOKEN}", "Content-Type": "application/json"},
        {"apikey": UAZAPI_ADMIN_TOKEN, "Content-Type": "application/json"},
    ]
    bodies = ({"number": e164}, {"numbers": [e164]}, {"phone": e164})
    async with httpx.AsyncClient(timeout=20) as cx:
        for h in headers_try:
            for b in bodies:
                try:
                    r = await cx.post(UAZAPI_CHECK_URL, json=b, headers=h)
                    if 200 <= r.status_code < 300:
                        d = r.json() if r.content else {}
                        if isinstance(d, dict):
                            if any(k in d for k in ("exists","valid","is_whatsapp")):
                                return bool(d.get("exists") or d.get("valid") or d.get("is_whatsapp"))
                            arr = d.get("data") or d.get("numbers") or []
                            if arr and isinstance(arr[0], dict):
                                i = arr[0]
                                return bool(i.get("exists") or i.get("valid") or i.get("is_whatsapp"))
                except Exception:
                    continue
    return None

# ---------- REST (com fallback) ----------
@app.get("/leads")
async def leads(
    nicho: str,
    local: str,
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(1, description="1=filtrar só WhatsApp; 0=retornar todos"),
):
    try:
        nums, exhausted = collect_numbers_info(nicho, local, n)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(nums)
    if verify:
        sem = asyncio.Semaphore(12)
        async def job(tel):
            async with sem:
                ok = await check_whatsapp(tel)
                return tel if ok else None
        results = await asyncio.gather(*[job(t) for t in nums])
        wa_items = [t for t in results if t]
        wa_count = len(wa_items)
        non_wa_count = searched - wa_count
        return {
            "count": wa_count,
            "items": [{"phone": t} for t in wa_items],
            "searched": searched,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "exhausted": exhausted or (searched < n),
        }

    # verify=0 → retorna todos
    return {
        "count": searched,
        "items": [{"phone": t} for t in nums],
        "searched": searched,
        "wa_count": None,
        "non_wa_count": None,
        "exhausted": exhausted or (searched < n),
    }

# ---------- SSE (progresso em tempo real) ----------
@app.get("/leads/stream")
def leads_stream(
    nicho: str,
    local: str,
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(1),
):
    async def agen():
        yield f"event: start\ndata: {json.dumps({'target': n})}\n\n"
        searched = 0
        wa_count = 0
        non_wa_count = 0

        sem = asyncio.Semaphore(12)

        async def wa_check(tel):
            if not verify:
                return True  # se não filtrar, considere todos "válidos" para listar
            res = await check_whatsapp(tel)
            return bool(res)

        try:
            # processa em série com limite (por página já vem em lotes do coletor)
            async def process_tel(tel):
                nonlocal searched, wa_count, non_wa_count
                ok = await wa_check(tel)
                searched += 1
                if ok:
                    wa_count += 1
                    # envia item (somente WhatsApp)
                    yield f"event: item\ndata: {json.dumps({'phone': tel, 'count': wa_count, 'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"
                else:
                    non_wa_count += 1
                # envia progresso sempre
                yield f"event: progress\ndata: {json.dumps({'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"

            # Consumir o gerador síncrono como assíncrono
            loop = asyncio.get_event_loop()
            for tel in iter_numbers(nicho, local, n):
                async for chunk in process_tel(tel):
                    yield chunk
        finally:
            exhausted = (wa_count < n)  # se não atingiu a meta, assumimos esgotado na origem
            payload = {"count": wa_count, "searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count, "exhausted": exhausted}
            yield f"event: done\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(agen(), media_type="text/event-stream")
