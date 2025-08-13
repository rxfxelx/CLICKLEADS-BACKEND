import os
import re
import json
import asyncio
from typing import AsyncGenerator, List, Set, Dict, Any, Tuple

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from collector import collect_numbers

app = FastAPI(title="Lead Extractor API", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _sse(event: str, data: Dict[str, Any]) -> bytes:
    return f"event: {event}\n".encode() + f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

# ----------------- VERIFICAÇÃO UAZAPI (INSTANCE) -----------------
async def _post_uazapi(numbers: List[str], form: str, headers: Dict[str, str], client: httpx.AsyncClient) -> Tuple[Set[str], Dict[str, Any]]:
    """Envia 50 por vez. form: 'digits' ou 'e164'."""
    ok: Set[str] = set()
    meta = {"batches": 0, "sent": 0, "ok": 0, "fail": 0, "mode": form, "errors": [], "last_status": None}

    BATCH = 50
    THROTTLE_MS = 120

    for i in range(0, len(numbers), BATCH):
        chunk = numbers[i:i+BATCH]
        payload = {"numbers": chunk}
        try:
            r = await client.post(UAZAPI_CHECK_URL, json=payload, headers=headers)
            meta["last_status"] = r.status_code
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            meta["fail"] += len(chunk)
            meta["errors"].append(f"http_err:{type(e).__name__}")
            await asyncio.sleep(THROTTLE_MS / 1000)
            meta["batches"] += 1
            continue

        rows = data if isinstance(data, list) else data.get("data") or data.get("numbers") or []
        if isinstance(rows, dict):
            rows = rows.get("numbers", [])

        for item in rows:
            q = str(item.get("query") or item.get("number") or item.get("phone") or "")
            qd = _digits(q)
            is_wa = bool(item.get("isInWhatsapp") or item.get("is_whatsapp") or item.get("exists") or item.get("valid"))
            if is_wa:
                if form == "digits":
                    ok.add(qd)  # no modo digits comparamos por dígitos
                else:
                    ok.add(q if q.startswith("+") else f"+{qd}")

        meta["ok"] += len(ok)
        meta["sent"] += len(chunk)
        meta["batches"] += 1
        await asyncio.sleep(THROTTLE_MS / 1000)

    return ok, meta

async def verify_whatsapp(numbers_e164: List[str]) -> Tuple[Set[str], Dict[str, Any]]:
    """
    1) Tenta com DIGITS (55119...).
    2) Se 0 positivos, tenta com E.164 (+55119...).
    Retorna (set_wa_em_E164, meta).
    """
    meta_all = {"modes": []}
    if not numbers_e164 or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return set(), {"modes": [], "errors": ["missing_env_or_numbers"]}

    # prepara duas listas
    digits_list = [_digits(n) for n in numbers_e164 if _digits(n)]
    e164_list   = [n if n.startswith("+") else f"+{_digits(n)}" for n in numbers_e164 if _digits(n)]

    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}
    TIMEOUT = httpx.Timeout(15.0)

    wa_e164: Set[str] = set()
    async with httpx.AsyncClient(timeout=TIMEOUT) as cx:
        # modo 1: digits
        ok_digits, m1 = await _post_uazapi(digits_list, "digits", headers, cx)
        meta_all["modes"].append(m1)

        if ok_digits:
            # converte dígitos -> E164
            wa_e164 = {("+" + d) for d in ok_digits}
        else:
            # modo 2: e164
            ok_e164, m2 = await _post_uazapi(e164_list, "e164", headers, cx)
            meta_all["modes"].append(m2)
            wa_e164 = ok_e164

    return wa_e164, meta_all

# ----------------- HEALTH -----------------
@app.get("/health")
def health():
    return {"ok": True}

# ----------------- JSON -----------------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        loop = asyncio.get_running_loop()
        candidates, exhausted_all = await loop.run_in_executor(None, lambda: collect_numbers(nicho, local, n, overscan_mult=8))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(candidates)

    if verify == 1:
        wa_set, wa_meta = await verify_whatsapp(candidates)
        wa_list = [p for p in candidates if p in wa_set][:n]
        wa_count = len(wa_list)
        non_wa_count = searched - wa_count
        return {
            "count": wa_count,
            "items": [{"phone": p, "has_whatsapp": True} for p in wa_list],
            "searched": searched,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "exhausted": (exhausted_all and wa_count < n),
            "wa_meta": wa_meta,
        }

    out = candidates[:n]
    return {
        "count": len(out),
        "items": [{"phone": p, "has_whatsapp": None} for p in out],
        "searched": searched,
        "wa_count": 0,
        "non_wa_count": searched,
        "exhausted": exhausted_all and (len(out) < n),
    }

# ----------------- SSE -----------------
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
            candidates, exhausted_all = await loop.run_in_executor(None, lambda: collect_numbers(nicho, local, n, overscan_mult=8))
        except Exception as e:
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set, wa_meta = await verify_whatsapp(candidates)
            wa_list = [p for p in candidates if p in wa_set][:n]
            wa_count = len(wa_list)
            non_wa_count = searched - wa_count

            yield _sse("progress", {"searched": searched, "wa_count": wa_count, "non_wa_count": non_wa_count})
            for p in wa_list:
                yield _sse("item", {"phone": p, "has_whatsapp": True})

            yield _sse("done", {
                "count": wa_count,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count,
                "searched": searched,
                "exhausted": (exhausted_all and wa_count < n),
                "wa_meta": wa_meta,
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
            "exhausted": exhausted_all and (len(out) < n),
        })

    return StreamingResponse(gen(), media_type="text/event-stream")
