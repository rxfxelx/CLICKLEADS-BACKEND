import os
import re
import json
import asyncio
from typing import AsyncGenerator, List, Set, Dict, Any

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

from collector import collect_numbers

# -------------------- APP & CORS --------------------
app = FastAPI(title="Lead Extractor API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# -------------------- ENV --------------------
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

# -------------------- UTILS --------------------
def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

def _sse(event: str, data: Dict[str, Any]) -> bytes:
    return f"event: {event}\n".encode() + f"data: {json.dumps(data, ensure_ascii=False)}\n\n".encode()

# -------------------- UAZAPI VERIFY (INSTANCE TOKEN) --------------------
async def verify_whatsapp(numbers: List[str]) -> Set[str]:
    """
    Envia lotes para a Uazapi usando *instance token* no header `token`
    e interpreta retorno conforme doc (lista de objetos com `query`, `isInWhatsapp`).
    Retorna os números em E.164 que têm WhatsApp.
    """
    ok: Set[str] = set()
    if not numbers or not UAZAPI_CHECK_URL or not UAZAPI_INSTANCE_TOKEN:
        return ok

    # mapa dígitos -> E164
    dmap: Dict[str, str] = {}
    for n in numbers:
        d = _digits(n)
        if d:
            dmap[d] = n if n.startswith("+") else f"+{d}"

    digits = list(dmap.keys())
    if not digits:
        return ok

    headers = {
        "Content-Type": "application/json",
        "token": UAZAPI_INSTANCE_TOKEN,  # <<< INSTANCE TOKEN
    }

    BATCH = 50          # seguro p/ Uazapi
    THROTTLE_MS = 120   # respeitar rate
    TIMEOUT = httpx.Timeout(15.0)

    async with httpx.AsyncClient(timeout=TIMEOUT) as cx:
        for i in range(0, len(digits), BATCH):
            chunk = digits[i:i+BATCH]
            try:
                r = await cx.post(UAZAPI_CHECK_URL, json={"numbers": chunk}, headers=headers)
                r.raise_for_status()
                data = r.json()
            except Exception:
                await asyncio.sleep(THROTTLE_MS/1000)
                continue

            # doc: normalmente uma LISTA
            rows = data if isinstance(data, list) else data.get("data") or data.get("numbers") or []
            if isinstance(rows, dict):
                rows = rows.get("numbers", [])

            for item in rows:
                qd = _digits(str(item.get("query") or item.get("number") or ""))
                if item.get("isInWhatsapp") and qd in dmap:
                    ok.add(dmap[qd])

            await asyncio.sleep(THROTTLE_MS/1000)

    return ok

# -------------------- HEALTH --------------------
@app.get("/health")
def health():
    return {"ok": True}

# -------------------- JSON ENDPOINT --------------------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        loop = asyncio.get_running_loop()
        # coletar um pool maior p/ compensar filtro do WhatsApp
        candidates, exhausted_all = await loop.run_in_executor(
            None, lambda: collect_numbers(nicho, local, n, overscan_mult=8)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    searched = len(candidates)

    if verify == 1:
        wa_set = await verify_whatsapp(candidates)
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
        }

    # sem verificação
    out = candidates[:n]
    return {
        "count": len(out),
        "items": [{"phone": p, "has_whatsapp": None} for p in out],
        "searched": searched,
        "wa_count": 0,
        "non_wa_count": searched,
        "exhausted": exhausted_all and (len(out) < n),
    }

# -------------------- SSE ENDPOINT --------------------
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
            yield _sse("error", {"error": f"collector_error: {type(e).__name__}"})
            return

        searched = len(candidates)

        if verify == 1:
            wa_set = await verify_whatsapp(candidates)
            wa_list = [p for p in candidates if p in wa_set][:n]
            wa_count = len(wa_list)
            non_wa_count = searched - wa_count

            # progress
            yield _sse("progress", {
                "searched": searched,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count
            })

            # stream dos itens verificados
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
