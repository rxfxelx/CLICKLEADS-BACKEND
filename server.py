import os, json, asyncio
from typing import List, Dict, Any
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse
import httpx
from anyio import to_thread

from collector import collect_numbers_batch

APP = FastAPI(title="Lead Extractor API", version="2.0.0")
APP.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r".*",
    allow_methods=["GET","OPTIONS"],
    allow_headers=["*"],
)

DEBUG = os.getenv("DEBUG","0")=="1"
def _dbg(*a):
    if DEBUG: print("[server]", *a, flush=True)

UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL","").rstrip("/")
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN","")

# ---------- helpers ----------

def _split_cities(local: str) -> List[str]:
    # "BH, Contagem , Betim" -> ["BH","Contagem","Betim"]
    return [c.strip() for c in local.split(",") if c.strip()]

async def _uazapi_check_bulk(e164_list: List[str]) -> Dict[str,bool]:
    """Retorna {+5511...: True/False}. Usa lotes de 100."""
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return {n: True for n in e164_list}  # sem verificação -> permite tudo

    out: Dict[str,bool] = {}
    url = UAZAPI_CHECK_URL
    headers_variants = [
        {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
        {"Authorization": f"Bearer {UAZAPI_INSTANCE_TOKEN}", "Content-Type": "application/json"},
        {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"},
    ]

    async with httpx.AsyncClient(timeout=30) as cx:
        for i in range(0, len(e164_list), 100):
            chunk = e164_list[i:i+100]
            ok = False
            for h in headers_variants:
                try:
                    r = await cx.post(url, json={"numbers": chunk}, headers=h)
                    if not (200 <= r.status_code < 300):
                        continue
                    data = r.json()
                    # aceita lista simples [{"query": "...", "isInWhatsapp": true}, ...]
                    if isinstance(data, list):
                        for item in data:
                            q = item.get("query") or item.get("number")
                            out[q] = bool(item.get("isInWhatsapp") or item.get("is_whatsapp") or item.get("valid") or item.get("exists"))
                        ok = True; break
                    # aceita dict {"data":[...]} ou {"numbers":[...]}
                    arr = data.get("data") or data.get("numbers")
                    if isinstance(arr, list):
                        for item in arr:
                            q = item.get("query") or item.get("number")
                            out[q] = bool(item.get("isInWhatsapp") or item.get("is_whatsapp") or item.get("valid") or item.get("exists"))
                        ok = True; break
                except Exception as e:
                    _dbg("uazapi chunk error:", type(e).__name__)
            if not ok:
                # se não deu, marca como False pra esse chunk (evita travar)
                for n in chunk:
                    out[n] = False
    return out

async def _collect_round(nicho: str, cities: List[str], want: int, starts: Dict[str,int]) -> Dict[str,Any]:
    """Roda o coletor em thread pra não bloquear o loop."""
    phones, searched, new_starts, exhausted = await to_thread.run_sync(
        collect_numbers_batch, nicho, cities, want, starts
    )
    return {"phones": phones, "searched": searched, "starts": new_starts, "exhausted": exhausted}

# ---------- endpoints ----------

@APP.get("/health")
def health():
    return {"ok": True}

@APP.get("/leads")
async def leads(nicho: str = Query(...), local: str = Query(...), n: int = Query(50, ge=1, le=500), verify: int = Query(0)):
    if not nicho.strip(): raise HTTPException(400, "nicho vazio")
    if not local.strip(): raise HTTPException(400, "local vazio")
    cities = _split_cities(local)
    if not cities: raise HTTPException(400, "local inválido")

    want = n
    starts: Dict[str,int] = {c:0 for c in cities}
    wa_only = verify == 1

    wa_total = 0; non_wa_total = 0
    collected: List[str] = []

    while len(collected) < want:
        round_need = max(50, want - len(collected))  # busca em blocos
        r = await _collect_round(nicho, cities, round_need, starts)
        starts = r["starts"]
        found = r["phones"]
        searched = r["searched"]
        exhausted = r["exhausted"]
        _dbg("round found=", len(found), "searched=", searched, "exhausted=", exhausted)

        if not found:
            return {"count": len(collected), "items":[{"phone":p} for p in collected],
                    "searched": searched, "wa_count": wa_total, "non_wa_count": non_wa_total, "exhausted": True}

        if wa_only:
            verdicts = await _uazapi_check_bulk(found)
            wa = [p for p in found if verdicts.get(p, False)]
            nonwa = [p for p in found if not verdicts.get(p, False)]
            wa_total += len(wa); non_wa_total += len(nonwa)
            for p in wa:
                if p not in collected:
                    collected.append(p)
                    if len(collected) >= want: break
        else:
            for p in found:
                if p not in collected:
                    collected.append(p)
                    if len(collected) >= want: break

        if exhausted and len(collected) < want:
            break

    return {
        "count": len(collected),
        "items": [{"phone": p} for p in collected],
        "searched": len(collected),  # aproximação amigável
        "wa_count": wa_total,
        "non_wa_count": non_wa_total,
        "exhausted": len(collected) < want
    }

@APP.get("/leads/stream")
async def leads_stream(nicho: str = Query(...), local: str = Query(...), n: int = Query(50, ge=1, le=500), verify: int = Query(0)):

    async def gen():
        # validação
        if not nicho.strip() or not local.strip():
            yield "event: error\ndata: " + json.dumps({"error":"parâmetros inválidos"}) + "\n\n"; return

        cities = _split_cities(local)
        if not cities:
            yield "event: error\ndata: " + json.dumps({"error":"local inválido"}) + "\n\n"; return

        want = n; wa_only = verify == 1
        starts = {c:0 for c in cities}
        wa_total = 0; non_wa_total = 0
        collected: List[str] = []
        yield "event: start\ndata: {}\n\n"

        try:
            while len(collected) < want:
                round_need = max(50, want - len(collected))
                r = await _collect_round(nicho, cities, round_need, starts)
                starts = r["starts"]
                found = r["phones"]
                searched = r["searched"]
                exhausted = r["exhausted"]

                yield "event: progress\ndata: " + json.dumps({
                    "searched": searched, "wa_count": wa_total, "non_wa_count": non_wa_total
                }) + "\n\n"

                if not found:
                    break

                if wa_only:
                    verdicts = await _uazapi_check_bulk(found)
                    for p in found:
                        has = bool(verdicts.get(p, False))
                        if has and p not in collected:
                            collected.append(p)
                            yield "event: item\ndata: " + json.dumps({"phone": p, "has_whatsapp": True}) + "\n\n"
                            if len(collected) >= want: break
                    wa_total = len(collected)
                    non_wa_total += len([p for p in found if not verdicts.get(p, False)])
                else:
                    for p in found:
                        if p not in collected:
                            collected.append(p)
                            yield "event: item\ndata: " + json.dumps({"phone": p, "has_whatsapp": False}) + "\n\n"
                            if len(collected) >= want: break

                if exhausted and len(collected) < want:
                    break

            yield "event: done\ndata: " + json.dumps({
                "count": len(collected),
                "wa_count": wa_total,
                "non_wa_count": non_wa_total,
                "searched": len(collected),
                "exhausted": len(collected) < want
            }) + "\n\n"
        except Exception as e:
            yield "event: error\ndata: " + json.dumps({"error": f"{type(e).__name__}: {str(e)}"}) + "\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")
