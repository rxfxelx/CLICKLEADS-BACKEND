# server.py
import os
import json
import asyncio
import re
from typing import AsyncGenerator, List, Tuple, Iterable

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import httpx

from collector import collect_numbers  # seu collector original

# -------------------------
# Config & helpers
# -------------------------
def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _normalize_check_url(raw: str) -> str:
    """Garante https://<sub>.uazapi.com/chat/check"""
    raw = (raw or "").strip().rstrip("/")
    if not raw:
        return ""
    # já veio completo?
    if raw.endswith("/chat/check"):
        return raw
    # veio só domínio do sub
    if raw.endswith(".uazapi.com"):
        return raw + "/chat/check"
    # veio com /chat?
    if raw.endswith("/chat"):
        return raw + "/check"
    # fallback: mantém o que vier e adiciona /chat/check
    return raw + "/chat/check"

UAZAPI_CHECK_URL = _normalize_check_url(os.getenv("UAZAPI_CHECK_URL", ""))
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

BATCH = int(os.getenv("UAZAPI_BATCH_SIZE", "50"))
HTTP_TIMEOUT = int(os.getenv("UAZAPI_TIMEOUT", "30"))

def _log(*args):
    # logs vão para stdout no Railway
    print("[WA]", *args, flush=True)

# -------------------------
# FastAPI
# -------------------------
app = FastAPI(title="Lead Extractor API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {
        "ok": True,
        "uazapi_url": UAZAPI_CHECK_URL or "(not set)",
        "token_set": bool(UAZAPI_INSTANCE_TOKEN),
    }

# -------------------------
# UAZAPI parsing
# -------------------------
def _parse_uazapi_rows(payload) -> Iterable[dict]:
    """
    Aceita:
      - [ { query, isInWhatsapp, ... }, ... ]
      - { data: [ {...} ] }
      - { data: { numbers: [ {...} ] } }
      - { result: [ {...} ] }
    """
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return payload["data"]
        if isinstance(payload.get("result"), list):
            return payload["result"]
        d = payload.get("data")
        if isinstance(d, dict):
            rows = d.get("numbers") or d.get("list") or d.get("items")
            if isinstance(rows, list):
                return rows
    return []

async def verify_whatsapp(numbers: List[str]) -> Tuple[set, dict]:
    """
    Recebe E.164 (+55...), envia **dígitos** para UAZAPI e devolve:
      (set_com_WA_em_E164, meta_info)
    """
    meta = {"batches": 0, "sent": 0, "ok": 0, "fail": 0}
    if not numbers:
        return set(), meta
    if not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        _log("Config faltando: UAZAPI_CHECK_URL ou UAZAPI_INSTANCE_TOKEN")
        return set(), meta

    # mapa dígitos -> E164 (+)
    dmap = {}
    for n in numbers:
        d = _digits(n)
        if d:
            dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return set(), meta

    wa_plus = set()
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}

    timeout = httpx.Timeout(HTTP_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as cx:
        for i in range(0, len(digits), BATCH):
            batch = digits[i:i + BATCH]
            meta["batches"] += 1
            meta["sent"] += len(batch)
            try:
                r = await cx.post(UAZAPI_CHECK_URL, json={"numbers": batch}, headers=headers)
                status = r.status_code
                # log leve por batch
                _log(f"POST {UAZAPI_CHECK_URL}  status={status}  batch={len(batch)}")
                r.raise_for_status()
                data = r.json()
                rows = _parse_uazapi_rows(data)
                got = 0
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
                        got += 1
                _log(f"parsed_ok rows={len(list(rows)) if isinstance(rows, list) else 'iter'}  wa_found_in_batch={got}")
                meta["ok"] += 1
            except Exception as e:
                meta["fail"] += 1
                try:
                    txt = r.text  # type: ignore
                except Exception:
                    txt = ""
                _log(f"batch_error: {type(e).__name__}  detail={str(e)[:200]}  body={txt[:200]}")

    return wa_plus, meta

# -------------------------
# REST: leads (JSON)
# -------------------------
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
        _log("collector_error:", type(e).__name__, str(e)[:200])
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
            "wa_meta": meta,  # ajuda no debug
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

# -------------------------
# SSE: leads/stream
# -------------------------
def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")

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
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=8)
            )
        except Exception as e:
            _log("collector_error:", type(e).__name__, str(e)[:200])
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set, meta = await verify_whatsapp(candidates)
            wa_list = [p for p in candidates if p in wa_set][:n]
            wa_count = len(wa_list)
            non_wa_count = searched - wa_count

            yield _sse("progress", {
                "searched": searched,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count
            })

            for p in wa_list:
                yield _sse("item", {"phone": p, "has_whatsapp": True})

            exhausted = exhausted_all and (wa_count < n)
            yield _sse("done", {
                "count": wa_count,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "searched": searched,
                "exhausted": exhausted,
                "wa_meta": meta
            })
            return

        # sem verificação
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

# -------------------------
# Rota de debug da UAZAPI
# -------------------------
@app.get("/wa/debug")
async def wa_debug(numbers: str = Query(..., description="Ex: 5511999999999,5531999999999")):
    nums = [n.strip() for n in numbers.split(",") if n.strip()]
    wa, meta = await verify_whatsapp([f"+{n}" if not n.startswith("+") else n for n in nums])
    return {"input": nums, "match": sorted(list(wa)), "meta": meta, "url": UAZAPI_CHECK_URL}
