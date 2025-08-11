# server.py — SSE com keep-alive e UAZAPI via TOKEN DE INSTÂNCIA
import os, json, asyncio, time
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from collector import collect_numbers_info, iter_numbers  # agora com multi-cidades

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")  # ex: https://helsenia.uazapi.com/chat/check
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN")        # token DA INSTÂNCIA (não admin)

app = FastAPI(title="Smart Leads API", version="1.9.0")

# CORS (restrinja ao seu domínio se quiser)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# ------------ WhatsApp check (instância) ------------
async def check_whatsapp(e164: str) -> bool | None:
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return None
    headers_try = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
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

# ------------ REST (fallback) ------------
@app.get("/leads")
async def leads(
    nicho: str,
    local: str,
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(1, description="1=retorna só WhatsApp; 0=retorna todos"),
):
    try:
        nums, exhausted = collect_numbers_info(nicho, local, n)  # multi-cidades + esgotado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(nums)
    if verify:
        sem = asyncio.Semaphore(12)  # limite de concorrência na UAZAPI
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

    return {
        "count": searched,
        "items": [{"phone": t} for t in nums],
        "searched": searched,
        "wa_count": None,
        "non_wa_count": None,
        "exhausted": exhausted or (searched < n),
    }

# ------------ SSE (tempo real) ------------
@app.get("/leads/stream")
def leads_stream(
    nicho: str,
    local: str,                           # pode ser "BH, Contagem, Betim"
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(1, description="1=emitir apenas números com WhatsApp"),
):
    async def agen():
        # evento inicial
        yield f"event: start\ndata: {json.dumps({'target': n})}\n\n"

        searched = 0
        wa_count = 0
        non_wa_count = 0

        last_ping = time.time()

        async def wa_ok(tel: str) -> bool:
            if not verify:
                return True
            res = await check_whatsapp(tel)
            return bool(res)

        try:
            for tel in iter_numbers(nicho, local, n):  # multi-cidades por baixo
                searched += 1
                if await wa_ok(tel):
                    wa_count += 1
                    # item (apenas WA quando verify=1)
                    yield "event: item\n"
                    yield f"data: {json.dumps({'phone': tel, 'count': wa_count, 'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"
                else:
                    non_wa_count += 1

                # progresso (sempre)
                yield "event: progress\n"
                yield f"data: {json.dumps({'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"

                # keep-alive a cada ~15s para proxies não derrubarem a conexão
                if time.time() - last_ping > 15:
                    yield ": keepalive\n\n"
                    last_ping = time.time()
        finally:
            exhausted = (wa_count < n)
            payload = {
                "count": wa_count,
                "searched": searched,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "exhausted": exhausted
            }
            yield f"event: done\ndata: {json.dumps(payload)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # evita buffering em proxies tipo Nginx/Cloudflare
    }
    # SSE precisa de text/event-stream e blocos \n\n. :contentReference[oaicite:1]{index=1}
    return StreamingResponse(agen(), media_type="text/event-stream; charset=utf-8", headers=headers)
