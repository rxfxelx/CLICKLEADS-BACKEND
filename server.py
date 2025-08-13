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

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

UAZAPI_CHECK_URL        = (os.getenv("UAZAPI_CHECK_URL", "") or "").strip().rstrip("/")
UAZAPI_INSTANCE_TOKEN   = (os.getenv("UAZAPI_INSTANCE_TOKEN", "") or "").strip()

UAZAPI_BATCH_SIZE       = _env_int("UAZAPI_BATCH_SIZE", 50)          # <= 100 da doc funciona bem
UAZAPI_MAX_CONCURRENCY  = _env_int("UAZAPI_MAX_CONCURRENCY", 3)
UAZAPI_RETRIES          = _env_int("UAZAPI_RETRIES", 2)
UAZAPI_THROTTLE_MS      = _env_int("UAZAPI_THROTTLE_MS", 120)
UAZAPI_TIMEOUT          = _env_int("UAZAPI_TIMEOUT", 12)
OVERSCAN_MULT           = _env_int("OVERSCAN_MULT", 8)

# normaliza URL: se vier só domínio, acrescenta /chat/check
if UAZAPI_CHECK_URL and not UAZAPI_CHECK_URL.endswith("/chat/check"):
    # se já tem caminho diferente, mantemos; se é puro domínio, anexa
    if UAZAPI_CHECK_URL.count("/") <= 2:  # https://host
        UAZAPI_CHECK_URL = UAZAPI_CHECK_URL + "/chat/check"

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="Lead Extractor API", version="2.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Util
# -----------------------------------------------------------------------------
def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y", "sim"}
    return False

def _split_chunks(seq: Iterable[str], size: int) -> List[List[str]]:
    seq = list(seq)
    return [seq[i:i+size] for i in range(0, len(seq), max(1, size))]

# -----------------------------------------------------------------------------
# Verificação WhatsApp (robusta a formatos da API)
# -----------------------------------------------------------------------------
async def _verify_batch(
    client: httpx.AsyncClient,
    digits: List[str],
) -> Tuple[set, Dict]:
    """
    Envia um único lote (digits somente) para a UAZAPI.
    Retorna (set_e164_wa, meta_parcial).
    """
    wa_plus: set = set()
    meta = {"sent": len(digits), "ok": 0, "fail": 0}

    if not digits:
        return wa_plus, meta

    payload = {"numbers": digits}
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}

    # retentativas com backoff simples
    attempt = 0
    last_exc = None
    while attempt <= UAZAPI_RETRIES:
        try:
            r = await client.post(UAZAPI_CHECK_URL, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
            meta["ok"] += 1
            rows = []
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = data.get("data") or data.get("numbers") or []
                if isinstance(rows, dict):
                    rows = rows.get("numbers", [])

            for item in rows:
                q = str(item.get("query") or item.get("number") or item.get("phone") or "")
                qd = _digits(q)
                is_wa = (
                    _truthy(item.get("isInWhatsapp"))
                    or _truthy(item.get("is_whatsapp"))
                    or _truthy(item.get("exists"))
                    or _truthy(item.get("valid"))
                    or bool(item.get("jid"))           # jid preenchido é bom indício
                    or bool(str(item.get("verifiedName") or "").strip())
                )
                if is_wa and qd:
                    # devolvemos em E.164
                    wa_plus.add("+" + qd)
            return wa_plus, meta
        except Exception as e:
            last_exc = e
            meta["fail"] += 1
            # backoff pequeno + throttle
            await asyncio.sleep(0.4 + 0.15 * attempt)
            attempt += 1

    # falhou todas
    return wa_plus, meta

async def verify_whatsapp(numbers_e164: List[str]) -> Tuple[set, Dict]:
    """
    Recebe números em E.164 (+55...), envia apenas dígitos para a UAZAPI
    em lotes, com paralelismo limitado, retentativas e throttle.
    Retorna (set_e164_wa, meta_info).
    """
    meta_all = {"batches": 0, "sent": 0, "ok": 0, "fail": 0}
    if not numbers_e164 or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return set(), meta_all

    # mapeia dígitos <-> e164
    digits_list = []
    e164_by_digits = {}
    for n in numbers_e164:
        d = _digits(n)
        if not d:
            continue
        digits_list.append(d)
        e164_by_digits[d] = n if n.startswith("+") else f"+{d}"

    chunks = _split_chunks(digits_list, UAZAPI_BATCH_SIZE)
    meta_all["batches"] = len(chunks)
    meta_all["sent"] = len(digits_list)

    sem = asyncio.Semaphore(max(1, UAZAPI_MAX_CONCURRENCY))
    wa_all: set = set()

    async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
        async def worker(batch: List[str]):
            async with sem:
                wa_set, meta = await _verify_batch(client, batch)
                # normaliza resultado para e164 usando e164_by_digits
                for w in wa_set:
                    d = _digits(w)
                    if d in e164_by_digits:
                        wa_all.add(e164_by_digits[d])
                meta_all["ok"] += meta["ok"]
                meta_all["fail"] += meta["fail"]
                # throttle leve entre chamadas
                await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

        tasks = [asyncio.create_task(worker(b)) for b in chunks]
        if tasks:
            await asyncio.gather(*tasks)

    return wa_all, meta_all

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True}

def _sse(event: str, data: dict) -> bytes:
    return f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")

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
        yield _sse("start", {})

        # coleta (pool maior para compensar filtro WA)
        try:
            loop = asyncio.get_running_loop()
            candidates, exhausted_all = await loop.run_in_executor(
                None, lambda: collect_numbers(nicho, local, n, overscan_mult=OVERSCAN_MULT)
            )
        except Exception as e:
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        # sem verificação: stream direto
        if verify == 0:
            for p in candidates[:n]:
                yield _sse("item", {"phone": p, "has_whatsapp": None})
            yield _sse("progress", {"searched": searched, "wa_count": 0, "non_wa_count": searched})
            yield _sse(
                "done",
                {
                    "count": min(n, len(candidates)),
                    "wa_count": 0,
                    "non_wa_count": searched,
                    "searched": searched,
                    "exhausted": exhausted_all and (len(candidates) < n),
                },
            )
            return

        # com verificação: por lotes (feedback contínuo)
        wa_total: List[str] = []
        wa_count = 0
        non_wa_count = 0
        wa_meta_total = {"batches": 0, "sent": 0, "ok": 0, "fail": 0}

        chunks = _split_chunks(candidates, UAZAPI_BATCH_SIZE)
        wa_meta_total["batches"] = len(chunks)
        wa_meta_total["sent"] = len(candidates)

        # Aplica o mesmo paralelismo, mas vamos emitir progresso após cada batch concluído
        sem = asyncio.Semaphore(max(1, UAZAPI_MAX_CONCURRENCY))

        async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
            async def proc_batch(batch: List[str]):
                nonlocal wa_count, non_wa_count, wa_total, wa_meta_total
                async with sem:
                    # mapeia batch -> dígitos
                    dmap = [_digits(x) for x in batch if _digits(x)]
                    sub_wa_set, meta = await _verify_batch(client, dmap)
                    wa_meta_total["ok"] += meta["ok"]
                    wa_meta_total["fail"] += meta["fail"]
                    # normaliza e envia itens
                    batch_wa = set()
                    for w in sub_wa_set:
                        d = _digits(w)
                        # recupera E164 original que pertence a este batch
                        for cand in batch:
                            if _digits(cand) == d:
                                batch_wa.add(cand)
                                break
                    # itens
                    for p in batch:
                        if p in batch_wa:
                            if len(wa_total) < n:
                                wa_total.append(p)
                                wa_count += 1
                                yield _sse("item", {"phone": p, "has_whatsapp": True})
                        else:
                            non_wa_count += 1
                    # progresso
                    yield _sse(
                        "progress",
                        {"searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count},
                    )
                    await asyncio.sleep(UAZAPI_THROTTLE_MS / 1000.0)

            # processa batches em paralelo limitado, na ordem de término
            # (mantém envio de progresso mesmo com paralelismo)
            for i in range(0, len(chunks), UAZAPI_MAX_CONCURRENCY):
                group = chunks[i : i + UAZAPI_MAX_CONCURRENCY]
                # roda o grupo e encaminha eventos conforme acabam
                coros = [proc_batch(b) async for b in _as_async_gen(group, proc_batch)]
                # o helper acima já yielda; aqui seguimos para o próximo grupo
                # mas precisamos limitar listagem: implementamos abaixo

        # done
        exhausted = exhausted_all and (wa_count < n)
        yield _sse(
            "done",
            {
                "count": wa_count,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "searched": searched,
                "exhausted": exhausted,
                "wa_meta": wa_meta_total,
            },
        )

    # helper: transforma lista de batches em gerador que executa e repassa os yields
    async def _as_async_gen(batches: List[List[str]], fn):
        for b in batches:
            async for ev in fn(b):   # fn(b) é um async generator (proc_batch)
                yield ev

    return StreamingResponse(gen(), media_type="text/event-stream")
