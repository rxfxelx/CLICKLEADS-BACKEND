import os
import json
import asyncio
from typing import Dict, List, Set, Tuple
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse
import httpx

from collector import collect_numbers_batch

# ========= Config UAZAPI =========
UAZAPI_SUBDOMAIN = os.getenv("UAZAPI_SUBDOMAIN", "").strip()
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN", "").strip()
UAZAPI_URL = f"https://{UAZAPI_SUBDOMAIN}.uazapi.com/chat/check" if UAZAPI_SUBDOMAIN else ""

# ========= FastAPI =========
app = FastAPI(title="Smart Leads API", version="2.0.0")

# CORS: libera produção e previews do Vercel
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ========= Helpers =========
def split_cities(local: str) -> List[str]:
    # ex: "Belo Horizonte, Contagem, Betim"
    cities = [c.strip() for c in local.split(",") if c.strip()]
    return cities or [local.strip()]

async def verify_whatsapp_batch(numbers: List[str]) -> Set[str]:
    """
    Verifica em lote na UAZAPI (única chamada). Retorna um set com os números que têm WhatsApp.
    Se não houver configuração, retorna set vazio e o caller decide o que fazer.
    """
    if not (UAZAPI_URL and UAZAPI_INSTANCE_TOKEN and numbers):
        return set()

    # UAZAPI aceita {"numbers": [..]} e retorna uma lista de objetos com "query", "isInWhatsapp".
    payload = {"numbers": numbers}
    headers = {"token": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=30) as cx:
        try:
            r = await cx.post(UAZAPI_URL, headers=headers, json=payload)
            if r.status_code // 100 != 2:
                return set()
            data = r.json()
            out = set()
            if isinstance(data, list):
                for item in data:
                    try:
                        if item.get("isInWhatsapp") is True:
                            q = str(item.get("query") or "").strip()
                            if q:
                                out.add(q)
                    except Exception:
                        continue
            return out
        except Exception:
            return set()

def json_event(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode("utf-8")

# ========= Endpoints =========
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
async def leads(
    nicho: str = Query(..., min_length=1),
    local: str = Query(..., min_length=1),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    """
    Coleta números e (opcional) verifica WhatsApp em lote.
    Se verify=1, tentamos continuar coletando até atingir n números com WhatsApp
    ou esgotar as páginas.
    """
    cities = split_cities(local)
    # Estado de paginação por cidade (start offset)
    start_by_city: Dict[str, int] = {c: 0 for c in cities}

    collected_global: List[str] = []
    wa_set: Set[str] = set()
    wa_count = 0
    non_wa_count = 0
    exhausted_all = False

    # Quantos por rodada de coleta (buffer maior ajuda quando vamos filtrar por WA)
    batch_target = max(60, min(200, n * 2))

    # Loop até atingir n (se verify=1) ou até reunir n (verify=0), ou esgotar
    while True:
        # coleta síncrona no thread pool (para não misturar Playwright sync com loop async)
        numbers, searched_round, start_by_city, exhausted_round = await asyncio.to_thread(
            collect_numbers_batch,
            nicho,
            cities,
            batch_target,
            start_by_city
        )

        # Adiciona coletados deduplicando na ordem
        for tel in numbers:
            if tel not in collected_global:
                collected_global.append(tel)

        if verify == 1:
            # verifica tudo que ainda não verificamos
            to_check = [t for t in collected_global if t not in wa_set]
            chk = await verify_whatsapp_batch(to_check)
            wa_set.update(chk)
            wa_count = len(wa_set)
            non_wa_count = max(0, len(collected_global) - wa_count)

            # se já atingimos n WhatsApp, paramos
            if wa_count >= n:
                exhausted_all = False
                break
        else:
            # sem verificar: paramos quando atingirmos n números brutos
            if len(collected_global) >= n:
                exhausted_all = False
                break

        # se rodada esgotou geral e não atingimos objetivo, finaliza
        if exhausted_round:
            exhausted_all = True
            break

        # aumenta um pouco o alvo da próxima rodada (progressivo)
        batch_target = min(500, batch_target + max(20, n // 2))

    # Monta resposta
    if verify == 1:
        # entrega no máximo n com WhatsApp
        wa_list = [p for p in collected_global if p in wa_set]
        wa_list = wa_list[:n]
        items = [{"phone": p, "has_whatsapp": True} for p in wa_list]
        return JSONResponse({
            "count": len(items),
            "items": items,
            "searched": len(collected_global),
            "wa_count": len(wa_list),
            "non_wa_count": max(0, len(collected_global) - len(wa_list)),
            "exhausted": exhausted_all
        })
    else:
        items = [{"phone": p, "has_whatsapp": None} for p in collected_global[:n]]
        return JSONResponse({
            "count": len(items),
            "items": items,
            "searched": len(collected_global),
            "wa_count": 0,
            "non_wa_count": 0,
            "exhausted": exhausted_all
        })

@app.get("/leads/stream")
async def leads_stream(
    nicho: str = Query(..., min_length=1),
    local: str = Query(..., min_length=1),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, ge=0, le=1),
):
    """
    Versão SSE: envia start, progress, item (após verificação), done.
    Verificação é feita **após** cada rodada de coleta.
    """

    async def gen():
        try:
            yield json_event("start", {})
            cities = split_cities(local)
            start_by_city: Dict[str, int] = {c: 0 for c in cities}

            collected_global: List[str] = []
            wa_set: Set[str] = set()
            wa_count = 0
            non_wa_count = 0
            exhausted_all = False

            batch_target = max(60, min(200, n * 2))

            while True:
                numbers, searched_round, start_by_city, exhausted_round = await asyncio.to_thread(
                    collect_numbers_batch,
                    nicho, cities, batch_target, start_by_city
                )

                # junta/conta
                new_count = 0
                for tel in numbers:
                    if tel not in collected_global:
                        collected_global.append(tel)
                        new_count += 1

                # progresso bruto (antes de verificar)
                yield json_event("progress", {
                    "searched": len(collected_global),
                    "wa_count": wa_count,
                    "non_wa_count": non_wa_count
                })

                if verify == 1:
                    to_check = [t for t in collected_global if t not in wa_set]
                    chk = await verify_whatsapp_batch(to_check)
                    wa_set.update(chk)
                    wa_count = len(wa_set)
                    non_wa_count = max(0, len(collected_global) - wa_count)

                    # envia os itens (apenas WA)
                    for p in collected_global:
                        if p in wa_set:
                            yield json_event("item", {"phone": p, "has_whatsapp": True})

                    if wa_count >= n:
                        exhausted_all = False
                        break
                else:
                    # envia os itens brutos
                    for p in collected_global:
                        yield json_event("item", {"phone": p, "has_whatsapp": None})

                    if len(collected_global) >= n:
                        exhausted_all = False
                        break

                if exhausted_round:
                    exhausted_all = True
                    break

                batch_target = min(500, batch_target + max(20, n // 2))

            # finalize
            if verify == 1:
                wa_list = [p for p in collected_global if p in wa_set][:n]
                yield json_event("done", {
                    "count": len(wa_list),
                    "wa_count": len(wa_list),
                    "non_wa_count": max(0, len(collected_global) - len(wa_list)),
                    "searched": len(collected_global),
                    "exhausted": exhausted_all
                })
            else:
                raw = collected_global[:n]
                yield json_event("done", {
                    "count": len(raw),
                    "wa_count": 0,
                    "non_wa_count": 0,
                    "searched": len(collected_global),
                    "exhausted": exhausted_all
                })

        except Exception as e:
            yield json_event("error", {"error": f"{type(e).__name__}: {e}"})

    return StreamingResponse(gen(), media_type="text/event-stream")
