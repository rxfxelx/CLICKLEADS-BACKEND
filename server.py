import os
import re
import json
import asyncio
from typing import AsyncGenerator, Dict, Iterable, List, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from collector import collect_numbers

# -------------------------------------------------------------------
# ENV / helpers
# -------------------------------------------------------------------
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

UAZAPI_CHECK_URL        = (os.getenv("UAZAPI_CHECK_URL", "") or "").strip().rstrip("/")
UAZAPI_INSTANCE_TOKEN   = (os.getenv("UAZAPI_INSTANCE_TOKEN", "") or "").strip()

UAZAPI_BATCH_SIZE       = _env_int("UAZAPI_BATCH_SIZE", 50)
UAZAPI_MAX_CONCURRENCY  = _env_int("UAZAPI_MAX_CONCURRENCY", 3)
UAZAPI_RETRIES          = _env_int("UAZAPI_RETRIES", 2)
UAZAPI_THROTTLE_MS      = _env_int("UAZAPI_THROTTLE_MS", 120)
UAZAPI_TIMEOUT          = _env_int("UAZAPI_TIMEOUT", 12)
OVERSCAN_MULT           = _env_int("OVERSCAN_MULT", 8)

# Se veio só domínio, completa com /chat/check
if UAZAPI_CHECK_URL and not UAZAPI_CHECK_URL.endswith("/chat/check"):
    if UAZAPI_CHECK_URL.count("/") <= 2:
        UAZAPI_CHECK_URL = UAZAPI_CHECK_URL + "/chat/check"

def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _truthy(v) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return v != 0
    if isinstance(v, str): return v.strip().lower() in {"true", "1", "yes", "y", "sim"}
    return False

def _split_chunks(seq: Iterable[str], size: int) -> List[List[str]]:
    seq = list(seq)
    size = max(1, size)
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def _sse(event: str, data: dict) -> bytes:
    return (f"event: {event}\n" f"data: {json.dumps(data, ensure_ascii=False)}\n\n").encode("utf-8")

# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------
app = FastAPI(title="Lead Extractor API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# -------------------------------------------------------------------
# UAZAPI callers
# -------------------------------------------------------------------
async def _verify_batch_once(client: httpx.AsyncClient, digits: List[str]) -> List[Dict]:
    payload = {"numbers": digits}
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}
    r = await client.post(UAZAPI_CHECK_URL, json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()
    rows = []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("data") or data.get("numbers") or []
        if isinstance(rows, dict):
            rows = rows.get("numbers", [])
    return rows if isinstance(rows, list) else []

async def _verify_batch_retry(client: httpx.AsyncClient, digits: List[str]) -> List[Dict]:
    last_exc = None
    for attempt in range(UAZAPI_RETRIES + 1):
        try:
            return await _verify_batch_once(client, digits)
        except Exception as e:
            last_exc = e
            await asyncio.sleep(0.4 + attempt * 0.2)
    return []  # falhou

async def verify_whatsapp(numbers_e164: List[str]) -> Tuple[set, Dict]:
    """
    Modo REST (não-stream): paraleliza moderadamente (MAX_CONCURRENCY).
    Retorna (set_e164_wa, meta).
    """
    meta = {"batches": 0, "sent": 0, "ok": 0, "fail": 0}
    if not numbers_e164 or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return set(), meta

    e164_by_digits: Dict[str, str] = {}
    digits_all: List[str] = []
    for n in numbers_e164:
        d = _digits(n)
        if d:
            e164_by_digits[d] = n if n.startswith("+") else f"+{d}"
            digits_all.append(d)

    chunks = _split_chunks(digits_all, UAZAPI_BATCH_SIZE)
    meta["batches"] = len(chunks)
    meta["sent"] = len(digits_all)

    sem = asyncio.Semaphore(max(1, UAZAPI_MAX_CONCURRENCY))
    wa_all: set = set()

    async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
        async def worker(batch: List[str]):
            nonlocal wa_all
            async with sem:
                rows = await _verify_batch_retry(client, batch)
                ok = 1 if rows else 0
                meta["ok"] += ok
                meta["fail"] += (1 - ok)
                for item in rows:
                    q = str(item.get("query") or item.get("number") or item.get("phone") or "")
                    qd = _digits(q)
                    is_wa = (
                        _truthy(item.get("isInWhatsapp"))
                        or _truthy(item.get("is_whatsapp"))
                        or _truthy(item.get("exists"))
                        or _truthy(item.get("valid"))
                        or bool(item.get("jid"))
                        or bool(str(item.get("verifiedName") or "").strip())
                    )
                    if is_wa and qd in e164_by_digits:
                        wa_all.add(e164_by_digits[qd])
                await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

        tasks = [asyncio.create_task(worker(b)) for b in chunks]
        if tasks:
            await asyncio.gather(*tasks)

    return wa_all, meta

# -------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------
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
            None, lambda: collect_numbers(nicho, local, n, overscan_mult=OVERSCAN_MULT)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(candidates)

    if verify == 1:
        wa_set, wa_meta = await verify_whatsapp(candidates)
        wa_list = [p for p in candidates if p in wa_set][:n]
        wa_count = len(wa_list)
        non_wa_count = searched - wa_count
        exhausted = exhausted_all and (wa_count < n)
        return {
            "count": wa_count,
            "items": [{"phone": p, "has_whatsapp": True} for p in wa_list],
            "searched": searched,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "exhausted": exhausted,
            "wa_meta": wa_meta,
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
    async def gen() -> AsyncGenerator[bytes, None]:
        # start
        yield _sse("start", {})

        # coleta
        try:
            loop = asyncio.get_running_loop()
            candidates, exhausted_all = await loop.run_in_executor(
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=OVERSCAN_MULT)
            )
        except Exception as e:
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        # sem verificação
        if verify == 0:
            for p in candidates[:n]:
                yield _sse("item", {"phone": p, "has_whatsapp": None})
            yield _sse("progress", {"searched": searched, "wa_count": 0, "non_wa_count": searched})
            yield _sse("done", {
                "count": min(n, len(candidates)),
                "wa_count": 0,
                "non_wa_count": searched,
                "searched": searched,
                "exhausted": exhausted_all and (len(candidates) < n)
            })
            return

        # com verificação — sequencial (estável pro SSE)
        wa_list: List[str] = []
        wa_count = 0
        non_wa_count = 0

        chunks = _split_chunks(candidates, UAZAPI_BATCH_SIZE)
        async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
            for batch in chunks:
                dlist = [_digits(x) for x in batch if _digits(x)]
                rows = await _verify_batch_retry(client, dlist)

                batch_wa_digits = set()
                for item in rows:
                    q = str(item.get("query") or item.get("number") or item.get("phone") or "")
                    qd = _digits(q)
                    is_wa = (
                        _truthy(item.get("isInWhatsapp"))
                        or _truthy(item.get("is_whatsapp"))
                        or _truthy(item.get("exists"))
                        or _truthy(item.get("valid"))
                        or bool(item.get("jid"))
                        or bool(str(item.get("verifiedName") or "").strip())
                    )
                    if is_wa and qd:
                        batch_wa_digits.add(qd)

                for p in batch:
                    if _digits(p) in batch_wa_digits:
                        if len(wa_list) < n:
                            wa_list.append(p)
                            wa_count += 1
                            yield _sse("item", {"phone": p, "has_whatsapp": True})
                    else:
                        non_wa_count += 1

                yield _sse("progress", {"searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count})
                await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

                if len(wa_list) >= n:
                    break

        exhausted = exhausted_all and (wa_count < n)
        yield _sse("done", {
            "count": wa_count,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "searched": searched,
            "exhausted": exhausted
        })

    return StreamingResponse(gen(), media_type="text/event-stream; charset=utf-8")
