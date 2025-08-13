import os
import re
import json
import asyncio
from typing import List, AsyncGenerator, Tuple

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from collector import collect_numbers

# ---------------------
# Config / ENV
# ---------------------
UAZAPI_CHECK_URL       = os.getenv("UAZAPI_CHECK_URL", "").strip().rstrip("/")
UAZAPI_INSTANCE_TOKEN  = os.getenv("UAZAPI_INSTANCE_TOKEN", "").strip()
UAZAPI_SUBDOMAIN       = os.getenv("UAZAPI_SUBDOMAIN", "").strip()

OVERSCAN_MULT          = int(os.getenv("OVERSCAN_MULT", "8"))         # pool maior para filtrar WA
BATCH                  = int(os.getenv("UAZAPI_BATCH_SIZE", "50"))    # tamanho do lote para /chat/check
HTTP_TIMEOUT           = float(os.getenv("UAZAPI_TIMEOUT", "15"))     # timeout por request (s)
UAZAPI_THROTTLE_MS     = int(os.getenv("UAZAPI_THROTTLE_MS", "0"))    # delay entre lotes

# ---------------------
# App
# ---------------------
app = FastAPI(title="Lead Extractor API", version="2.1.0")

# CORS (Vercel)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------
# Utils
# ---------------------
def _log(msg: str) -> None:
    print(msg, flush=True)

def _sse_event(event: str, data: dict) -> bytes:
    return (f"event: {event}\n" f"data: {json.dumps(data, ensure_ascii=False)}\n\n").encode("utf-8")

def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _parse_uazapi_rows(data) -> list:
    """
    Suporta diferentes formatos que a UAZAPI pode retornar.
    Idealmente, é uma lista de objetos [{query, isInWhatsapp, ...}, ...]
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        if "numbers" in data and isinstance(data["numbers"], list):
            return data["numbers"]
        # algumas integrações aninham de outra forma
        if "data" in data and isinstance(data["data"], dict):
            maybe = data["data"].get("numbers")
            if isinstance(maybe, list):
                return maybe
    return []

# ---------------------
# UAZAPI verify
# ---------------------
async def verify_whatsapp(numbers: List[str]) -> Tuple[set, dict]:
    """
    Recebe E.164 (+55...) e verifica pela UAZAPI em lotes.
    Retorna (set de números E.164 que têm WA, meta com contadores).
    """
    meta = {"batches": 0, "sent": 0, "ok": 0, "fail": 0}
    if not numbers:
        return set(), meta
    if not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        _log("[WA] config faltando (UAZAPI_CHECK_URL/UAZAPI_INSTANCE_TOKEN)")
        return set(), meta

    # mapa dígitos -> E.164
    dmap = {}
    for n in numbers:
        d = _digits(n)
        if d:
            dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return set(), meta

    wa_plus = set()
    headers = {
        "Content-Type": "application/json",
        "token": UAZAPI_INSTANCE_TOKEN,                         # alguns tenants aceitam 'token'
        "Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}",     # outros exigem Bearer
    }

    timeout = httpx.Timeout(HTTP_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as cx:
        for i in range(0, len(digits), BATCH):
            batch = digits[i:i + BATCH]
            meta["batches"] += 1
            meta["sent"] += len(batch)

            payload = {"numbers": batch}
            if UAZAPI_SUBDOMAIN:
                payload["subdomain"] = UAZAPI_SUBDOMAIN  # para hosts multi-tenant como hia-clientes.uazapi.com

            try:
                r = await cx.post(UAZAPI_CHECK_URL, json=payload, headers=headers)
                status = r.status_code
                _log(f"[WA] POST {UAZAPI_CHECK_URL} status={status} batch={len(batch)}")
                r.raise_for_status()

                data = r.json()
                rows = _parse_uazapi_rows(data)

                found = 0
                for item in rows:
                    q = str(item.get("query") or item.get("number") or item.get("phone") or "")
                    qd = _digits(q)
                    is_wa = bool(
                        item.get("isInWhatsapp")
                        or item.get("is_whatsapp")
                        or item.get("exists")
                        or item.get("valid")
                    )
                    if is_wa and qd in dmap:
                        wa_plus.add(dmap[qd])
                        found += 1

                _log(f"[WA] parsed rows={len(rows)} wa_found_batch={found}")
                meta["ok"] += 1
            except Exception as e:
                meta["fail"] += 1
                body = ""
                try:
                    body = r.text  # type: ignore
                except Exception:
                    pass
                _log(f"[WA] batch_error: {type(e).__name__} {str(e)[:160]} body={body[:160]}")

            if UAZAPI_THROTTLE_MS > 0:
                await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

    return wa_plus, meta

# ---------------------
# Endpoints
# ---------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "uazapi_url_set": bool(UAZAPI_CHECK_URL),
        "token_set": bool(UAZAPI_INSTANCE_TOKEN),
        "subdomain_set": bool(UAZAPI_SUBDOMAIN),
        "overscan_mult": OVERSCAN_MULT,
        "batch": BATCH,
        "timeout": HTTP_TIMEOUT,
    }

@app.get("/wa/debug")
async def wa_debug(numbers: str = Query(..., description="CSV de E.164 ou dígitos")):
    raw = [x.strip() for x in numbers.split(",") if x.strip()]
    # normaliza para E.164 (se vierem só dígitos)
    norm = []
    for r in raw:
        d = _digits(r)
        if not d:
            continue
        norm.append("+" + d if not r.startswith("+") else r)

    wa_set, meta = await verify_whatsapp(norm)
    return {"input": norm, "wa": sorted(list(wa_set)), "wa_count": len(wa_set), "meta": meta}

@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        loop = asyncio.get_running_loop()
        # pool maior para compensar o filtro do WhatsApp
        candidates, exhausted_all = await loop.run_in_executor(
            None, lambda: collect_numbers(nicho, local, n, overscan_mult=OVERSCAN_MULT)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(candidates)

    if verify == 1:
        wa_set, meta = await verify_whatsapp(candidates)
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
            "wa_meta": meta,
        }

    # sem verificação
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
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=OVERSCAN_MULT)
            )
        except Exception as e:
            yield _sse_event("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set, meta = await verify_whatsapp(candidates)
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
                "exhausted": exhausted,
                "wa_meta": meta,
            })
            return

        # sem verificação: stream direto
        out = candidates[:n]
        for p in out:
            yield _sse_event("item", {"phone": p, "has_whatsapp": None})
        yield _sse_event("progress", {"searched": searched, "wa_count": 0, "non_wa_count": searched})
        yield _sse_event("done", {
            "count": len(out),
            "wa_count": 0,
            "non_wa_count": searched,
            "searched": searched,
            "exhausted": exhausted_all and (len(out) < n)
        })

    return StreamingResponse(gen(), media_type="text/event-stream")
