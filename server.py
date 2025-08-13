import re
import json
import asyncio
from typing import AsyncGenerator, List, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import httpx

from collector import collect_numbers

app = FastAPI(title="Lead Extractor API", version="2.2.0")

# =========================
# CORS: liberar geral (evita dor de cabeça no front)
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# =========================
# UAZAPI (hardcoded)
# =========================
# Base sem /chat/check — o código completa a rota
UAZAPI_CHECK_URL_BASE = "https://hia-clientes.uazapi.com"
UAZAPI_INSTANCE_TOKEN = "55b903f3-9c7c-4457-9ffd-7296a35d832e"

# Tuning
UAZAPI_BATCH_SIZE = 50
UAZAPI_MAX_CONCURRENCY = 3
UAZAPI_RETRIES = 2
UAZAPI_THROTTLE_MS = 120
UAZAPI_TIMEOUT = 12

def _build_check_url() -> str:
    base = (UAZAPI_CHECK_URL_BASE or "").rstrip("/")
    return base if base.endswith("/chat/check") else f"{base}/chat/check"

def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _sse(event: str, data: Dict[str, Any]) -> bytes:
    return (f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n").encode("utf-8")

# =========================
# Verificação WhatsApp (robusta)
# =========================
async def verify_whatsapp(numbers: List[str]) -> set[str]:
    """
    Recebe E.164 (+55...), envia dígitos para UAZAPI e devolve set em E.164 dos que têm WA.
    Usa token de instância via header 'token'.
    Aceita várias formas de resposta:
      - list
      - {data: [...]}
      - {numbers: [...]}
      - {data: {numbers: [...]}}
    Considera como WA se achar alguma das chaves: isInWhatsapp, is_whatsapp, exists, valid, whatsapp, inWhatsapp
    """
    url = _build_check_url()
    if not numbers or not url or not UAZAPI_INSTANCE_TOKEN:
        return set()

    # map dígitos -> E.164
    dmap: Dict[str, str] = {}
    for n in numbers:
        d = _digits(n)
        if d:
            dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return set()

    wa_plus = set()
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}

    async def _post_batch(cx: httpx.AsyncClient, batch: List[str]) -> List[Dict[str, Any]]:
        # retries + throttle
        for _ in range(UAZAPI_RETRIES + 1):
            try:
                r = await cx.post(url, json={"numbers": batch}, headers=headers)
                r.raise_for_status()
                data = r.json()
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict):
                    rows = data.get("data") or data.get("numbers") or []
                    if isinstance(rows, dict):
                        rows = rows.get("numbers", [])
                else:
                    rows = []
                if not isinstance(rows, list):
                    rows = []
                return rows
            except Exception:
                await asyncio.sleep(0.3)
        return []

    sem = asyncio.Semaphore(UAZAPI_MAX_CONCURRENCY)
    async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as cx:
        tasks = []
        for i in range(0, len(digits), UAZAPI_BATCH_SIZE):
            batch = digits[i : i + UAZAPI_BATCH_SIZE]
            async def _job(b=batch):
                async with sem:
                    rows = await _post_batch(cx, b)
                    if UAZAPI_THROTTLE_MS > 0:
                        await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)
                    return rows
            tasks.append(asyncio.create_task(_job()))
        all_rows_lists = await asyncio.gather(*tasks, return_exceptions=False)

    for rows in all_rows_lists:
        if not rows:
            continue
        for item in rows:
            if not isinstance(item, dict):
                continue
            q = str(item.get("query") or item.get("number") or item.get("phone") or "")
            qd = _digits(q)
            is_wa = bool(
                item.get("isInWhatsapp")
                or item.get("is_whatsapp")
                or item.get("exists")
                or item.get("valid")
                or item.get("whatsapp")
                or item.get("inWhatsapp")
            )
            if is_wa and qd in dmap:
                wa_plus.add(dmap[qd])

    return wa_plus

# =========================
# Endpoints
# =========================
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
        loop = asyncio.get_running_loop()
        candidates, exhausted_all = await loop.run_in_executor(
            None, lambda: collect_numbers(nicho, local, n, overscan_mult=None)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(candidates)

    if verify == 1:
        wa_set = await verify_whatsapp(candidates)
        items_wa = [p for p in candidates if p in wa_set][:n]
        wa_count = len(items_wa)
        non_wa_count = searched - wa_count
        exhausted = exhausted_all and (wa_count < n)
        return JSONResponse({
            "count": wa_count,
            "items": [{"phone": p, "has_whatsapp": True} for p in items_wa],
            "searched": searched,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "exhausted": exhausted,
        })

    items = candidates[:n]
    return JSONResponse({
        "count": len(items),
        "items": [{"phone": p, "has_whatsapp": None} for p in items],
        "searched": searched,
        "wa_count": 0,
        "non_wa_count": searched,
        "exhausted": exhausted_all and (len(items) < n),
    })

@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    async def gen() -> AsyncGenerator[bytes, None]:
        yield _sse("start", {})

        try:
            loop = asyncio.get_running_loop()
            candidates, exhausted_all = await loop.run_in_executor(
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=None)
            )
        except Exception as e:
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set = await verify_whatsapp(candidates)
            wa_list = [p for p in candidates if p in wa_set][:n]
            wa_count = len(wa_list)
            non_wa_count = searched - wa_count

            yield _sse("progress", {"searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count})
            for p in wa_list:
                yield _sse("item", {"phone": p, "has_whatsapp": True})

            exhausted = exhausted_all and (wa_count < n)
            yield _sse("done", {
                "count": wa_count,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "searched": searched,
                "exhausted": exhausted
            })
            return

        out = candidates[:n]
        for p in out:
            yield _sse("item", {"phone": p, "has_whatsapp": None})

        yield _sse("progress", {"searched": searched, "wa_count": 0, "non_wa_count": searched})
        yield _sse("done", {
            "count": len(out),
            "wa_count": 0,
            "non_wa_count": searched,
            "searched": searched,
            "exhausted": exhausted_all and (len(out) < n)
        })

    return StreamingResponse(gen(), media_type="text/event-stream")
