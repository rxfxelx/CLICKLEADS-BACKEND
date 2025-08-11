# server.py — usa TOKEN DA INSTÂNCIA (não o admin) no /chat/check
import os, json, asyncio
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from collector import collect_numbers_info, iter_numbers

# Vars de ambiente
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")  # ex: https://helsenia.uazapi.com/chat/check
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN")        # << token da INSTÂNCIA

app = FastAPI(title="Smart Leads API", version="1.8.0")

# CORS (ajuste para o domínio do seu front se quiser restringir)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# ---------- WhatsApp check (usa token da INSTÂNCIA) ----------
async def check_whatsapp(e164: str) -> bool | None:
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return None
    # Testa formatos aceitos pela UAZAPI (token/apikey/Bearer)
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
                            # formatos comuns de retorno: exists/valid/is_whatsapp ou array data/numbers
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
    verify: int = Query(1, description="1=retorna só WhatsApp; 0=retorna todos"),
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

    # verify=0 → retorna todos (sem filtrar por WhatsApp)
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
    verify: int = Query(1, description="1=emitir apenas números com WhatsApp"),
):
    async def agen():
        yield f"event: start\ndata: {json.dumps({'target': n})}\n\n"
        searched = 0
        wa_count = 0
        non_wa_count = 0

        async def wa_ok(tel: str) -> bool:
            if not verify:
                return True
            res = await check_whatsapp(tel)
            return bool(res)

        try:
            for tel in iter_numbers(nicho, local, n):
                searched += 1
                if await wa_ok(tel):
                    wa_count += 1
                    yield "event: item\n"
                    yield f"data: {json.dumps({'phone': tel, 'count': wa_count, 'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"
                else:
                    non_wa_count += 1
                yield "event: progress\n"
                yield f"data: {json.dumps({'searched': searched, 'wa_count': wa_count, 'non_wa_count': non_wa_count})}\n\n"
        finally:
            exhausted = (wa_count < n)
            payload = {"count": wa_count, "searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count, "exhausted": exhausted}
            yield f"event: done\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(agen(), media_type="text/event-stream")
