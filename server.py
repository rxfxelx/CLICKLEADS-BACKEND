# server.py
import os
import re
import json
import asyncio
from typing import AsyncGenerator, List

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import httpx

from collector import collect_numbers

app = FastAPI(title="Lead Extractor API", version="2.0.0")

# CORS (Vercel)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Config de verificação WhatsApp (UAZAPI)
# -----------------------------------------------------------------------------
_UAZAPI_CHECK_URL_RAW = (os.getenv("UAZAPI_CHECK_URL", "") or "").strip()
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "") or ""

def _normalize_check_url(raw: str) -> str:
    """
    Aceita:
      - "https://subdominio.uazapi.com/chat/check"
      - "subdominio.uazapi.com/chat/check"
      - "https://subdominio.uazapi.com"
      - "subdominio.uazapi.com"
    e normaliza para ".../chat/check".
    """
    if not raw:
        return ""
    url = raw
    if "://" not in url:
        url = "https://" + url.lstrip("/")
    url = url.rstrip("/")
    if not url.endswith("/chat/check"):
        url = url + "/chat/check"
    return url

UAZAPI_CHECK_URL = _normalize_check_url(_UAZAPI_CHECK_URL_RAW)

def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

# -----------------------------------------------------------------------------
# Verificação WhatsApp (robusta)
# -----------------------------------------------------------------------------
async def verify_whatsapp(numbers: List[str]) -> set[str]:
    """
    Recebe números em E.164 (+55...) e devolve um set dos que têm WhatsApp.
    - Envia dígitos para a UAZAPI (como pede a doc).
    - Faz chunk menor (50) com pequeno intervalo.
    - Repetições com backoff em 429/5xx ou falhas transitórias.
    - Faz parsing tolerante aos formatos de resposta.
    """
    if not numbers or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return set()

    # map dígitos -> E.164
    dmap = {}
    for n in numbers:
        d = _digits(n)
        if not d:
            continue
        dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return set()

    wa_plus: set[str] = set()
    CHUNK = 50
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        # a API aceita "token" no header; incluímos também Authorization por segurança
        "token": UAZAPI_INSTANCE_TOKEN,
        "Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}",
    }

    async with httpx.AsyncClient() as cx:
        for i in range(0, len(digits), CHUNK):
            batch = digits[i : i + CHUNK]

            # Retentativas com backoff
            data = None
            for attempt in range(4):
                try:
                    r = await cx.post(
                        UAZAPI_CHECK_URL,
                        json={"numbers": batch},
                        headers=headers,
                        timeout=30,
                    )
                    # Trata rate-limit e 5xx com retry
                    if r.status_code in (429, 502, 503, 504):
                        await asyncio.sleep(min(2 ** attempt, 8))
                        continue
                    r.raise_for_status()
                    data = r.json()
                    break
                except Exception:
                    if attempt == 3:
                        # desiste desse lote e segue para o próximo
                        data = None
                        break
                    await asyncio.sleep(min(2 ** attempt, 8))

            if not data:
                await asyncio.sleep(0.2)
                continue

            # Normaliza diferentes formatos de resposta
            rows = []
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = (
                    data.get("data")
                    or data.get("numbers")
                    or data.get("result")
                    or []
                )
                if isinstance(rows, dict):  # às vezes vem aninhado
                    rows = rows.get("numbers") or rows.get("data") or []

            for item in rows:
                q = str(
                    item.get("query")
                    or item.get("number")
                    or item.get("phone")
                    or item.get("jid")
                    or ""
                )
                qd = _digits(q)

                is_wa = any(
                    bool(item.get(k))
                    for k in (
                        "isInWhatsapp",
                        "isInWhatsApp",
                        "is_whatsapp",
                        "exists",
                        "valid",
                        "whatsapp",
                    )
                )
                if is_wa and qd in dmap:
                    wa_plus.add(dmap[qd])

            # pequeníssimo intervalo entre lotes para evitar throttle
            await asyncio.sleep(0.2)

    return wa_plus

# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "uazapi_url_ok": bool(UAZAPI_CHECK_URL),
        "uazapi_token_ok": bool(UAZAPI_INSTANCE_TOKEN),
    }

# -----------------------------------------------------------------------------
# Util SSE
# -----------------------------------------------------------------------------
def _sse_event(event: str, data: dict) -> bytes:
    return f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        loop = asyncio.get_running_loop()
        # Coleta um pool maior para compensar o filtro do WhatsApp
        candidates, exhausted_all = await loop.run_in_executor(
            None, lambda: collect_numbers(nicho, local, n, overscan_mult=8)
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
        return {
            "count": wa_count,
            "items": [{"phone": p, "has_whatsapp": True} for p in items_wa],
            "searched": searched,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "exhausted": exhausted,
        }

    # Sem verificação
    items = candidates[:n]
    return {
        "count": len(items),
        "items": [{"phone": p, "has_whatsapp": None} for p in items],
        "searched": searched,
        "wa_count": 0,
        "non_wa_count": searched,
        "exhausted": exhausted_all and (len(items) < n),
    }

@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    async def gen() -> AsyncGenerator[bytes, None]:
        # start
        yield _sse_event("start", {})

        try:
            loop = asyncio.get_running_loop()
            candidates, exhausted_all = await loop.run_in_executor(
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=8)
            )
        except Exception as e:
            yield _sse_event("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set = await verify_whatsapp(candidates)
            wa_list = [p for p in candidates if p in wa_set][:n]
            wa_count = len(wa_list)
            non_wa_count = searched - wa_count

            yield _sse_event("progress", {
                "searched": searched,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count
            })
            for p in wa_list:
                yield _sse_event("item", {"phone": p, "has_whatsapp": True})

            exhausted = exhausted_all and (wa_count < n)
            yield _sse_event("done", {
                "count": wa_count,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "searched": searched,
                "exhausted": exhausted
            })
            return

        # Sem verificação: stream direto
        out = candidates[:n]
        for p in out:
            yield _sse_event("item", {"phone": p, "has_whatsapp": None})
        yield _sse_event("progress", {
            "searched": searched,
            "wa_count": 0,
            "non_wa_count": searched
        })
        yield _sse_event("done", {
            "count": len(out),
            "wa_count": 0,
            "non_wa_count": searched,
            "searched": searched,
            "exhausted": exhausted_all and (len(out) < n)
        })

    return StreamingResponse(gen(), media_type="text/event-stream")
