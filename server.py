import os
import re
import json
import random
import asyncio
from typing import AsyncGenerator, List, Set, Dict

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from collector import iter_numbers  # gerador incremental por cidade

app = FastAPI(title="Smart Leads API", version="3.1.0")

# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",   # ajuste para seu domínio se quiser
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)

# ---------------- UAZAPI ENV ----------------
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

UAZAPI_BATCH_SIZE = int(os.getenv("UAZAPI_BATCH_SIZE", "50"))       # números por request
UAZAPI_MAX_CONCURRENCY = int(os.getenv("UAZAPI_MAX_CONCURRENCY", "3"))
UAZAPI_TIMEOUT = float(os.getenv("UAZAPI_TIMEOUT", "12"))
UAZAPI_RETRIES = int(os.getenv("UAZAPI_RETRIES", "2"))
UAZAPI_THROTTLE_MS = int(os.getenv("UAZAPI_THROTTLE_MS", "120"))    # pausa entre requests (ms)

# ---------------- COLETA ENV ----------------
COLLECT_STEP = int(os.getenv("COLLECT_STEP", "40"))                  # ritmo do coletor
OVERSCAN_MULT = int(os.getenv("OVERSCAN_MULT", "8"))                 # margem p/ compensar não-WA

DEBUG = os.getenv("DEBUG", "0") == "1"

# --------------- UTILS ---------------
def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _sse(event: str, data: dict) -> bytes:
    return (f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n").encode()

def _cities(local: str) -> List[str]:
    cs = [c.strip() for c in (local or "").split(",")]
    return [c for c in cs if c]

def _uaz_ready() -> bool:
    return bool(UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN)

# --------------- VERIFICAÇÃO UAZAPI ---------------
def _b(v):
    if isinstance(v, bool): return v
    if v is None: return False
    return str(v).lower() in ("true","1","yes","y")

def _normalize_flags(payload) -> Dict[str, bool]:
    """Aceita formatos comuns da UAZAPI e devolve { query/number/phone: bool }."""
    out: Dict[str, bool] = {}
    if isinstance(payload, list):
        for i in payload:
            q = i.get("query") or i.get("number") or i.get("phone")
            if q:
                out[q] = _b(i.get("isInWhatsapp") or i.get("exists") or i.get("valid") or i.get("is_whatsapp"))
        return out
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("numbers")
        if isinstance(data, list):
            for i in data:
                q = i.get("query") or i.get("number") or i.get("phone")
                if q:
                    out[q] = _b(i.get("isInWhatsapp") or i.get("exists") or i.get("valid") or i.get("is_whatsapp"))
            return out
        if isinstance(data, dict):
            arr = data.get("numbers") or []
            if isinstance(arr, list):
                for i in arr:
                    q = i.get("query") or i.get("number") or i.get("phone")
                    if q:
                        out[q] = _b(i.get("isInWhatsapp") or i.get("exists") or i.get("valid") or i.get("is_whatsapp"))
                return out
    return out

async def _verify_chunk(client: httpx.AsyncClient, chunk_digits: List[str]) -> Dict[str, bool]:
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}
    body = {"numbers": chunk_digits}
    last_err = None
    for attempt in range(UAZAPI_RETRIES + 1):
        try:
            r = await client.post(UAZAPI_CHECK_URL, json=body, headers=headers, timeout=UAZAPI_TIMEOUT)
            if r.status_code >= 500 or r.status_code == 429:
                raise httpx.HTTPStatusError("uazapi busy", request=r.request, response=r)
            r.raise_for_status()
            data = r.json()
            flags = _normalize_flags(data)
            if DEBUG:
                print(f"[UAZAPI] batch={len(chunk_digits)} ok={sum(1 for v in flags.values() if _b(v))} try={attempt}")
            if not flags and attempt < UAZAPI_RETRIES:
                await asyncio.sleep(0.6 * (attempt + 1) + random.random() * 0.4)
                continue
            return flags
        except Exception as e:
            last_err = e
            if DEBUG:
                print(f"[UAZAPI] error try={attempt}: {e}")
            if attempt < UAZAPI_RETRIES:
                await asyncio.sleep(0.6 * (attempt + 1) + random.random() * 0.4)
            else:
                return {d: False for d in chunk_digits}
    if DEBUG and last_err:
        print(f"[UAZAPI] final error: {last_err}")
    return {d: False for d in chunk_digits}

async def verify_numbers_uazapi(e164_list: List[str]) -> Dict[str, bool]:
    """Recebe E.164 (+55...), envia DÍGITOS para UAZAPI em lotes concorrentes e retorna {E.164: True/False}."""
    if not e164_list or not _uaz_ready():
        return {n: False for n in e164_list}

    d2e: Dict[str, str] = {}
    digits_list: List[str] = []
    for n in e164_list:
        d = _digits(n)
        if d:
            d2e[d] = n if n.startswith("+") else f"+{d}"
            digits_list.append(d)
    if not digits_list:
        return {n: False for n in e164_list}

    limits = httpx.Limits(max_keepalive_connections=UAZAPI_MAX_CONCURRENCY,
                          max_connections=UAZAPI_MAX_CONCURRENCY)
    sem = asyncio.Semaphore(UAZAPI_MAX_CONCURRENCY)
    out: Dict[str, bool] = {}

    async with httpx.AsyncClient(limits=limits, timeout=UAZAPI_TIMEOUT) as client:
        async def worker(chunk: List[str]):
            async with sem:
                flags = await _verify_chunk(client, chunk)
                for k, v in flags.items():
                    kd = _digits(k)
                    if kd in d2e:
                        out[d2e[kd]] = bool(_b(v))
                if UAZAPI_THROTTLE_MS > 0:
                    await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

        tasks = []
        for i in range(0, len(digits_list), UAZAPI_BATCH_SIZE):
            tasks.append(asyncio.create_task(worker(digits_list[i:i+UAZAPI_BATCH_SIZE])))
        await asyncio.gather(*tasks)

    return {n: out.get(n, False) for n in e164_list}

# --------------- HEALTH ---------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "uaz_ready": _uaz_ready(),
        "batch": UAZAPI_BATCH_SIZE,
        "concurrency": UAZAPI_MAX_CONCURRENCY,
    }

# --------------- /leads (JSON) ---------------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    locais = _cities(local)
    if not locais:
        raise HTTPException(status_code=400, detail="local inválido")

    seen: Set[str] = set()
    wa_list: List[str] = []
    searched = 0

    try:
        for city in locais:
            max_city_candidates = n * OVERSCAN_MULT
            buffer: List[str] = []

            for tel in iter_numbers(nicho, city, max_total=max_city_candidates, step=COLLECT_STEP):
                if tel in seen:
                    continue
                seen.add(tel)
                buffer.append(tel)

                if len(buffer) >= UAZAPI_BATCH_SIZE:
                    searched += len(buffer)
                    if verify == 1 and _uaz_ready():
                        wa_map = await verify_numbers_uazapi(buffer)
                        wa_list.extend([p for p, ok in wa_map.items() if ok])
                        if len(wa_list) >= n:
                            break
                    buffer.clear()

            if buffer and (verify == 1 and _uaz_ready()):
                searched += len(buffer)
                wa_map = await verify_numbers_uazapi(buffer)
                wa_list.extend([p for p, ok in wa_map.items() if ok])
                buffer.clear()

            if verify == 1 and len(wa_list) >= n:
                break
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    if verify == 1:
        items = [{"phone": p, "has_whatsapp": True} for p in wa_list[:n]]
        return {
            "count": len(items),
            "items": items,
            "searched": searched,
            "wa_count": len(wa_list),
            "non_wa_count": searched - len(wa_list),
            "exhausted": len(items) < n,
        }

    # verify=0
    first_n = list(seen)[:n]
    items = [{"phone": p, "has_whatsapp": None} for p in first_n]
    return {
        "count": len(items),
        "items": items,
        "searched": len(seen),
        "wa_count": 0,
        "non_wa_count": 0,
        "exhausted": len(items) < n,
    }

# --------------- /leads/stream (SSE) ---------------
@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    locais = _cities(local)
    if not locais:
        return StreamingResponse(iter([_sse("error", {"error":"local inválido"})]), media_type="text/event-stream")

    async def gen() -> AsyncGenerator[bytes, None]:
        seen: Set[str] = set()
        wa_count = 0
        non_wa_count = 0
        searched = 0

        yield _sse("start", {})

        try:
            for city in locais:
                max_city_candidates = n * OVERSCAN_MULT
                buffer: List[str] = []

                for tel in iter_numbers(nicho, city, max_total=max_city_candidates, step=COLLECT_STEP):
                    if tel in seen:
                        continue
                    seen.add(tel)
                    buffer.append(tel)

                    if len(buffer) >= UAZAPI_BATCH_SIZE:
                        searched += len(buffer)
                        if verify == 1 and _uaz_ready():
                            wa_map = await verify_numbers_uazapi(buffer)
                            wa_batch = [p for p, ok in wa_map.items() if ok]
                            non_wa_count += len(buffer) - len(wa_batch)

                            for p in wa_batch:
                                wa_count += 1
                                if wa_count <= n:
                                    yield _sse("item", {"phone": p, "has_whatsapp": True})

                            yield _sse("progress", {
                                "searched": searched,
                                "wa_count": wa_count,
                                "non_wa_count": non_wa_count
                            })

                            if wa_count >= n:
                                break
                        else:
                            # sem verificação: stream direto (até n)
                            for p in buffer[:max(0, n - wa_count)]:
                                yield _sse("item", {"phone": p, "has_whatsapp": None})
                            yield _sse("progress", {"searched": searched, "wa_count": 0, "non_wa_count": 0})
                        buffer.clear()

                # resto da cidade
                if buffer:
                    searched += len(buffer)
                    if verify == 1 and _uaz_ready():
                        wa_map = await verify_numbers_uazapi(buffer)
                        wa_batch = [p for p, ok in wa_map.items() if ok]
                        non_wa_count += len(buffer) - len(wa_batch)
                        for p in wa_batch:
                            wa_count += 1
                            if wa_count <= n:
                                yield _sse("item", {"phone": p, "has_whatsapp": True})
                        yield _sse("progress", {
                            "searched": searched,
                            "wa_count": wa_count,
                            "non_wa_count": non_wa_count
                        })
                    else:
                        for p in buffer[:max(0, n - wa_count)]:
                            yield _sse("item", {"phone": p, "has_whatsapp": None})
                        yield _sse("progress", {"searched": searched, "wa_count": 0, "non_wa_count": 0})
                    buffer.clear()

                if verify == 1 and wa_count >= n:
                    break

            exhausted = (verify == 1 and wa_count < n) or (verify == 0 and len(seen) < n)

            yield _sse("done", {
                "count": min(wa_count, n) if verify==1 else min(len(seen), n),
                "wa_count": wa_count if verify==1 else 0,
                "non_wa_count": non_wa_count if verify==1 else 0,
                "searched": searched,
                "exhausted": exhausted
            })
        except Exception as e:
            yield _sse("error", {"error": f"server_error: {type(e).__name__}"})

    return StreamingResponse(gen(), media_type="text/event-stream")
