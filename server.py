import os
import json
import asyncio
import re
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

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "")

def _digits(n: str) -> str:
    return re.sub(r"\D", "", n or "")

async def verify_whatsapp(numbers: List[str]) -> set[str]:
    """
    Recebe E.164 (+55...), envia dígitos para UAZAPI e devolve set em E.164 dos que têm WA.
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

    wa_plus = set()
    CHUNK = 100
    headers = {"Content-Type": "application/json", "token": UAZAPI_INSTANCE_TOKEN}

    async with httpx.AsyncClient(timeout=30) as cx:
        for i in range(0, len(digits), CHUNK):
            batch = digits[i : i + CHUNK]
            try:
                r = await cx.post(UAZAPI_CHECK_URL, json={"numbers": batch}, headers=headers)
                r.raise_for_status()
                data = r.json()
            except Exception:
                continue

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
                is_wa = bool(
                    item.get("isInWhatsapp")
                    or item.get("is_whatsapp")
                    or item.get("exists")
                    or item.get("valid")
                )
                if is_wa and qd in dmap:
                    wa_plus.add(dmap[qd])

    return wa_plus

@app.get("/health")
def health():
    ok = True
    return {"ok": ok}

def _sse_event(event: str, data: dict) -> bytes:
    return f"event: {event}\n" + f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        loop = asyncio.get_running_loop()
        # coletar um pool maior para compensar filtro do WhatsApp
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
            # progresso (mostra tudo que varremos e quantos WA de fato)
            yield _sse_event("progress", {
                "searched": searched,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count
            })
            # stream item a item
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

        # sem verificação: stream direto
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
