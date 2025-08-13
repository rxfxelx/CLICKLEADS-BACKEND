# server.py
import os
import re
import json
import asyncio
import math
from typing import AsyncGenerator, List, Dict, Any

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import httpx

from collector import collect_numbers

app = FastAPI(title="Lead Extractor API", version="2.0.0")

# CORS – ajuste se precisar liberar outros domínios
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

# ------------- Utils -------------
def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in {"true", "1", "yes", "y", "sim"}

def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

# ------------- Verificação WA (robusta) -------------
WA_KEYS = (
    "isInWhatsapp",
    "is_whatsapp",
    "isWhatsapp",
    "whatsapp",
    "inWhatsapp",
    "exists",
    "valid",
    "hasWhatsapp",
    "has_whatsapp",
)

def _pick_rows(payload: Any) -> List[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "result", "results", "numbers"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                vv = v.get("numbers")
                if isinstance(vv, list):
                    return vv
    return []

def _pick_query(item: dict) -> str:
    for k in ("query", "number", "phone", "jid", "input"):
        v = item.get(k)
        if v:
            return str(v)
    return ""

async def _post_check(
    client: httpx.AsyncClient,
    url: str,
    numbers: List[str],
    headers: Dict[str, str],
    token_in_body: bool,
    token_value: str,
) -> List[dict]:
    body = {"numbers": numbers}
    if token_in_body:
        body["token"] = token_value
    r = await client.post(url, json=body, headers=headers)
    r.raise_for_status()
    return _pick_rows(r.json())

async def verify_whatsapp(numbers: List[str]) -> set[str]:
    """
    Tenta diferentes formas de autenticação e formatos dos números.
    Retorna conjunto em E.164 dos que têm WhatsApp.
    """
    if not numbers or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return set()

    # mapa dígitos -> E.164
    dmap = {}
    for n in numbers:
        d = _digits(n)
        if d:
            dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return set()

    # Estratégias: headers e inclusão do token no body
    header_variants = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
        {"x-api-key": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
    ]
    token_in_body_variants = [False, True]

    # formatos de números: só dígitos e com "+"
    format_variants = [
        digits,
        [f"+{d}" for d in digits],
    ]

    CHUNK = 50
    MAX_RETRIES = 2
    wa_plus = set()

    async with httpx.AsyncClient(timeout=30) as cx:
        for fmt_numbers in format_variants:
            if wa_plus:
                break
            for headers in header_variants:
                if wa_plus:
                    break
                for tib in token_in_body_variants:
                    positives = set()
                    try:
                        for i in range(0, len(fmt_numbers), CHUNK):
                            batch = fmt_numbers[i : i + CHUNK]
                            # retry simples
                            tries = 0
                            while True:
                                try:
                                    rows = await _post_check(cx, UAZAPI_CHECK_URL, batch, headers, tib, UAZAPI_INSTANCE_TOKEN)
                                    break
                                except Exception:
                                    tries += 1
                                    if tries > MAX_RETRIES:
                                        rows = []
                                        break
                                    await asyncio.sleep(0.8 * tries)

                            for it in rows:
                                q = _digits(_pick_query(it))
                                if not q:
                                    continue
                                is_wa = any(_as_bool(it.get(k)) for k in WA_KEYS)
                                if is_wa and q in dmap:
                                    positives.add(dmap[q])

                            # respiro leve para evitar throttling
                            await asyncio.sleep(0.1)
                    except Exception:
                        positives = set()

                    if positives:
                        wa_plus |= positives
                        break  # já validou com essa estratégia

    return wa_plus

# ------------- Rotas -------------
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
    async def gen() -> AsyncGenerator[str, None]:
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
