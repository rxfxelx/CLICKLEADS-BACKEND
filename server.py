# server.py — v2.0.1 (fix: sem 'return value' em async generator)
import os, json, asyncio, time
import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from collector import init_state, collect_batch  # stateful, multi-cidades

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN")

app = FastAPI(title="Smart Leads API", version="2.0.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

# ---------- UAZAPI: verificação em lote ----------
async def batch_check_whatsapp(num_list: list[str]) -> set[str] | None:
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN) or not num_list:
        return None
    headers_try = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
    ]
    payloads = [
        {"numbers": num_list},
        {"number": num_list[0]},
    ]
    async with httpx.AsyncClient(timeout=30) as cx:
        for h in headers_try:
            for body in payloads:
                try:
                    r = await cx.post(UAZAPI_CHECK_URL, json=body, headers=h)
                    if 200 <= r.status_code < 300 and r.content:
                        data = r.json()
                        wa = set()
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict):
                                    tel = item.get("query") or item.get("number") or item.get("phone")
                                    is_wa = item.get("isInWhatsapp") or item.get("exists") or item.get("valid") or item.get("is_whatsapp")
                                    if tel and is_wa:
                                        wa.add(str(tel))
                        elif isinstance(data, dict):
                            if any(k in data for k in ("exists","valid","is_whatsapp","isInWhatsapp")):
                                if data.get("exists") or data.get("valid") or data.get("is_whatsapp") or data.get("isInWhatsapp"):
                                    wa.add(num_list[0])
                            arr = data.get("data") or data.get("numbers") or []
                            if isinstance(arr, list):
                                for item in arr:
                                    if isinstance(item, dict):
                                        tel = item.get("query") or item.get("number") or item.get("phone")
                                        is_wa = item.get("isInWhatsapp") or item.get("exists") or item.get("valid") or item.get("is_whatsapp")
                                        if tel and is_wa:
                                            wa.add(str(tel))
                        return wa
                except Exception:
                    continue
    return None

# ---------- Núcleo: coleta + verificação (RETORNA dados) ----------
async def solve_contacts_once(nicho: str, local: str, n: int, want_only_wa: bool):
    state = init_state(local)
    seen: set[str] = set()
    wa_final: list[str] = []
    searched_total = 0
    non_wa_count = 0
    exhausted_all = False

    def batch_size(remaining):  # coletar “gordo” para minimizar chamadas na UAZAPI
        return max(40, min(400, remaining * 2))

    while len(wa_final) < n and not exhausted_all:
        remaining_wa = n - len(wa_final)
        to_collect = batch_size(remaining_wa)

        batch, state, exhausted_all = collect_batch(nicho, state, to_collect)
        new_nums = [t for t in batch if t not in seen]
        for t in new_nums:
            seen.add(t)
        searched_total += len(new_nums)

        if not new_nums and exhausted_all:
            break
        if not new_nums:
            continue

        wa_set = await batch_check_whatsapp(new_nums) if want_only_wa else None
        if wa_set is None:
            wa_now = new_nums if not want_only_wa else []
            non_wa_now = 0 if not want_only_wa else len(new_nums)
        else:
            wa_now = [t for t in new_nums if t in wa_set]
            non_wa_now = len(new_nums) - len(wa_now)

        non_wa_count += non_wa_now

        for tel in wa_now:
            if len(wa_final) >= n:
                break
            wa_final.append(tel)

    exhausted_flag = exhausted_all or (len(wa_final) < n)
    return wa_final, searched_total, non_wa_count, exhausted_flag

# ---------- Núcleo: versão SSE (GERA eventos) ----------
async def solve_contacts_sse(nicho: str, local: str, n: int, want_only_wa: bool):
    state = init_state(local)
    seen: set[str] = set()
    wa_final: list[str] = []
    searched_total = 0
    non_wa_count = 0
    exhausted_all = False

    def batch_size(remaining):
        return max(40, min(400, remaining * 2))

    yield f"event: start\ndata: {json.dumps({'target': n})}\n\n"

    last_ping = time.time()
    try:
        while len(wa_final) < n and not exhausted_all:
            remaining_wa = n - len(wa_final)
            to_collect = batch_size(remaining_wa)

            batch, state, exhausted_all = collect_batch(nicho, state, to_collect)
            new_nums = [t for t in batch if t not in seen]
            for t in new_nums:
                seen.add(t)
            searched_total += len(new_nums)

            yield "event: collect\n"
            yield f"data: {json.dumps({'found_batch': len(new_nums), 'searched': searched_total})}\n\n"

            if not new_nums and exhausted_all:
                break
            if not new_nums:
                continue

            wa_set = await batch_check_whatsapp(new_nums) if want_only_wa else None
            if wa_set is None:
                wa_now = new_nums if not want_only_wa else []
                non_wa_now = 0 if not want_only_wa else len(new_nums)
            else:
                wa_now = [t for t in new_nums if t in wa_set]
                non_wa_now = len(new_nums) - len(wa_now)

            non_wa_count += non_wa_now

            yield "event: verify\n"
            yield f"data: {json.dumps({'checked': len(new_nums), 'wa_now': len(wa_now), 'non_wa_now': non_wa_now})}\n\n"

            for tel in wa_now:
                if len(wa_final) >= n:
                    break
                wa_final.append(tel)
                yield "event: item\n"
                yield f"data: {json.dumps({'phone': tel, 'count': len(wa_final), 'searched': searched_total, 'wa_count': len(wa_final), 'non_wa_count': non_wa_count})}\n\n"

            yield "event: progress\n"
            yield f"data: {json.dumps({'searched': searched_total, 'wa_count': len(wa_final), 'non_wa_count': non_wa_count})}\n\n"

            if time.time() - last_ping > 15:
                yield ": keepalive\n\n"
                last_ping = time.time()
    finally:
        exhausted_flag = exhausted_all or (len(wa_final) < n)
        payload = {
            "count": len(wa_final),
            "searched": searched_total,
            "wa_count": len(wa_final),
            "non_wa_count": non_wa_count,
            "exhausted": exhausted_flag,
        }
        yield f"event: done\ndata: {json.dumps(payload)}\n\n"

# ---------- REST ----------
@app.get("/leads")
async def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    try:
        # coleta rápida
        nums, exhausted = collect_numbers_info(nicho, local, n)
    except Exception as e:
        # agora devolve a mensagem da exceção pra debugar
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}: {e}")

    items = [{"phone": p} for p in nums]

    # verificação WA (se pedida)
    wa_count = 0
    non_wa_count = 0
    if verify == 1 and nums:
        is_wa = await bulk_check_whatsapp(nums)  # sua função atual de bulk (uazapi)
        wa_only = []
        for p, ok in zip(nums, is_wa):
            if ok is True:
                wa_only.append({"phone": p, "has_whatsapp": True})
                wa_count += 1
            elif ok is False:
                non_wa_count += 1
        items = wa_only  # mantém só WhatsApp

    return {
        "count": len(items),
        "items": items,
        "searched": len(nums),
        "wa_count": wa_count,
        "non_wa_count": non_wa_count,
        "exhausted": exhausted,
    }


# ---------- SSE ----------
@app.get("/leads/stream")
def leads_stream(
    nicho: str,
    local: str,
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(1, description="1=emitir apenas números com WhatsApp"),
):
    async def agen():
        async for chunk in solve_contacts_sse(nicho, local, n, want_only_wa=bool(verify)):
            yield chunk

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(agen(), media_type="text/event-stream; charset=utf-8", headers=headers)



