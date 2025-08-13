# server.py
import os
import json
from typing import Dict, Iterable, List, Tuple, Set  # <- Set adicionado

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

from collector import collect_numbers_batch

UAZAPI_CHECK_URL = (os.getenv("UAZAPI_CHECK_URL") or "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN") or ""

app = FastAPI(title="Lead Extractor API", version="1.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

def _verify_chunk(cx: httpx.Client, numbers: List[str]) -> Dict[str, bool]:
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN and numbers):
        return {n: False for n in numbers}
    headers_list = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
    ]
    body = {"numbers": numbers}
    for headers in headers_list:
        try:
            r = cx.post(UAZAPI_CHECK_URL, json=body, headers=headers, timeout=20)
            if not (200 <= r.status_code < 300):
                continue
            data = r.json()
            if isinstance(data, list):
                out = {}
                for i in data:
                    q = str(i.get("query") or "").lstrip("+")
                    iswa = bool(i.get("isInWhatsapp") or i.get("is_whatsapp") or i.get("valid") or i.get("exists"))
                    if q.startswith("55"):
                        q = "+" + q
                    out[q] = iswa
                return out
            arr = data.get("data") or data.get("numbers") or []
            if isinstance(arr, list):
                out = {}
                for i in arr:
                    q = str(i.get("query") or i.get("number") or "").lstrip("+")
                    iswa = bool(i.get("isInWhatsapp") or i.get("is_whatsapp") or i.get("valid") or i.get("exists"))
                    if q.startswith("55"):
                        q = "+" + q
                    out[q] = iswa
                return out
        except Exception:
            continue
    return {n: False for n in numbers}

def verify_whatsapp(numbers: List[str], chunk_size: int = 80) -> Tuple[List[str], int, int]:
    if not numbers:
        return [], 0, 0
    wa: List[str] = []
    wa_count = 0
    non_wa_count = 0
    with httpx.Client() as cx:
        for i in range(0, len(numbers), chunk_size):
            batch = numbers[i : i + chunk_size]
            result = _verify_chunk(cx, batch)
            for n in batch:
                if result.get(n, False):
                    wa.append(n); wa_count += 1
                else:
                    non_wa_count += 1
    return wa, wa_count, non_wa_count

def split_cities(local: str) -> List[str]:
    return [c.strip() for c in (local or "").split(",") if c.strip()]

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads/stream")
def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0),
):
    cities = split_cities(local)
    if not cities:
        raise HTTPException(400, "local inválido")

    def event_generator():
        yield {"event": "start", "data": "{}"}

        seen: Set[str] = set()
        start_by_city: Dict[str, int] = {c: 0 for c in cities}

        wa_count = 0
        non_wa_count = 0
        emitted = 0
        exhausted_all = False

        batch_collect = 120

        while emitted < n and not exhausted_all:
            remaining_collect = max(0, min(batch_collect, 3 * n - emitted))
            phones, searched, start_by_city, exhausted_all = collect_numbers_batch(
                nicho, cities, remaining_collect, start_by_city
            )
            phones = [p for p in phones if p not in seen]
            seen.update(phones)

            if not phones:
                yield {"event": "progress", "data": json.dumps({
                    "searched": wa_count + non_wa_count,
                    "wa_count": wa_count,
                    "non_wa_count": non_wa_count
                })}
                if exhausted_all:
                    break
                continue

            if verify == 1:
                wa_list, wa_add, non_add = verify_whatsapp(phones, chunk_size=80)
                wa_count += wa_add
                non_wa_count += non_add
                for num in wa_list:
                    if emitted >= n: break
                    yield {"event": "item", "data": json.dumps({"phone": num, "has_whatsapp": True})}
                    emitted += 1
            else:
                for num in phones:
                    if emitted >= n: break
                    yield {"event": "item", "data": json.dumps({"phone": num, "has_whatsapp": False})}
                    emitted += 1

            yield {"event": "progress", "data": json.dumps({
                "searched": wa_count + non_wa_count if verify == 1 else emitted,
                "wa_count": wa_count,
                "non_wa_count": non_wa_count
            })}

        yield {"event": "done", "data": json.dumps({
            "count": emitted,
            "wa_count": wa_count,
            "non_wa_count": non_wa_count,
            "searched": wa_count + non_wa_count if verify == 1 else emitted,
            "exhausted": exhausted_all
        })}

    return EventSourceResponse(event_generator())

@app.get("/leads")
def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0),
):
    cities = split_cities(local)
    if not cities:
        raise HTTPException(400, "local inválido")

    items: List[Dict[str, str | bool]] = []
    wa_count = 0
    non_wa_count = 0
    exhausted_all = False
    start_by_city: Dict[str, int] = {c: 0 for c in cities}
    seen: Set[str] = set()

    while len(items) < n and not exhausted_all:
        phones, _, start_by_city, exhausted_all = collect_numbers_batch(
            nicho, cities, min(120, 3 * n - len(items)), start_by_city
        )
        phones = [p for p in phones if p not in seen]
        seen.update(phones)

        if not phones:
            break

        if verify == 1:
            wa_list, wa_add, non_add = verify_whatsapp(phones, chunk_size=80)
            wa_count += wa_add
            non_wa_count += non_add
            for num in wa_list:
                if len(items) >= n: break
                items.append({"phone": num, "has_whatsapp": True})
        else:
            for num in phones:
                if len(items) >= n: break
                items.append({"phone": num, "has_whatsapp": False})

    return {
        "count": len(items),
        "items": items,
        "searched": wa_count + non_wa_count if verify == 1 else len(items),
        "wa_count": wa_count,
        "non_wa_count": non_wa_count,
        "exhausted": exhausted_all,
    }
    
