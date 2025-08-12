import os, json, math
from typing import Dict, List, Iterable
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import httpx

from collector import collect_numbers_ex

# ====== Config ======
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")  # ex: https://helsenia.uazapi.com/chat/check
UAZAPI_INSTANCE_TOKEN = os.getenv("UAZAPI_INSTANCE_TOKEN")        # instance token (use este!)
# Opcional: limite de batch por chamada na verificação
VERIFY_BATCH = int(os.getenv("VERIFY_BATCH", "150"))

app = FastAPI(title="Smart Leads API", version="2.2.0")

# CORS (Vercel + localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://.*",
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ====== WhatsApp check ======
def check_whatsapp_bulk(numbers: List[str]) -> Dict[str, bool]:
    """
    Faz UMA ou poucas chamadas em lote à UAZAPI.
    Retorna {e164: True/False} para cada número.
    Se não houver configuração, assume True para não bloquear o fluxo quando 'verify=1'.
    """
    if not (UAZAPI_CHECK_URL and UAZAPI_INSTANCE_TOKEN):
        return {n: True for n in numbers}  # sem verificação configurada → não filtra

    out: Dict[str, bool] = {}
    headers = {"apikey": UAZAPI_INSTANCE_TOKEN, "Content-Type": "application/json"}
    with httpx.Client(timeout=30) as cx:
        for i in range(0, len(numbers), VERIFY_BATCH):
            chunk = numbers[i : i + VERIFY_BATCH]
            try:
                r = cx.post(UAZAPI_CHECK_URL, headers=headers, json={"numbers": chunk})
                if r.status_code >= 200 and r.status_code < 300:
                    data = r.json()
                    # formatos possíveis
                    arr = None
                    if isinstance(data, dict):
                        if "numbers" in data and isinstance(data["numbers"], list):
                            arr = data["numbers"]
                        elif "data" in data and isinstance(data["data"], list):
                            arr = data["data"]
                    if isinstance(arr, list):
                        for item in arr:
                            if isinstance(item, dict):
                                num = item.get("number") or item.get("phone")
                                if not num:
                                    continue
                                exists = bool(item.get("exists") or item.get("valid") or item.get("is_whatsapp"))
                                out[num] = exists
                    else:
                        # fallback: se a API retornar um único dict
                        if isinstance(data, dict):
                            exists = bool(data.get("exists") or data.get("valid") or data.get("is_whatsapp"))
                            if exists and len(chunk) == 1:
                                out[chunk[0]] = True
                            elif len(chunk) == 1:
                                out[chunk[0]] = False
                else:
                    # em erro, não bloqueia
                    for n in chunk:
                        out[n] = True
            except Exception:
                for n in chunk:
                    out[n] = True
    # garante que todos tenham valor
    for n in numbers:
        out.setdefault(n, True)
    return out

# ====== Helpers ======
def _json_items(phones: List[str], mask_whatsapp: Dict[str, bool] | None) -> List[Dict]:
    items = []
    if mask_whatsapp is None:
        # verify=0 → has_whatsapp = None
        for p in phones:
            items.append({"phone": p, "has_whatsapp": None})
    else:
        for p in phones:
            items.append({"phone": p, "has_whatsapp": bool(mask_whatsapp.get(p, False))})
    return items

def _collect_and_maybe_verify(nicho: str, local: str, n: int, verify: bool):
    """
    Estratégia:
      - overfetch inicial (3x) para reduzir iterações
      - verifica em lote (se verify=1)
      - se faltar, tenta novas coletas até bater n ou esgotar
    """
    target = n
    total_wa: List[str] = []
    total_nonwa = 0
    searched_acc = 0
    exhausted_flag = False

    # para evitar duplicados em múltiplas rodadas de coleta
    already_seen: set[str] = set()

    # no máximo 6 iterações de busca (suficiente para 500 com overfetch)
    for _ in range(6):
        if len(total_wa) >= target:
            break

        need = target - len(total_wa)
        overfetch = max(60, min(600, need * 3))
        # coleta
        phones, exhausted, searched = collect_numbers_ex(nicho, local, overfetch)
        searched_acc += searched

        # filtra duplicados
        fresh = [p for p in phones if p not in already_seen]
        for p in fresh:
            already_seen.add(p)

        if not fresh and exhausted:
            exhausted_flag = True
            break

        if verify:
            mask = check_whatsapp_bulk(fresh)
            wa_now = [p for p in fresh if mask.get(p, False)]
            non_wa_now = len(fresh) - len(wa_now)
            total_nonwa += non_wa_now
            total_wa.extend(wa_now)
        else:
            total_wa.extend(fresh)

        if exhausted and len(total_wa) < target:
            exhausted_flag = True
            break

    # corta no alvo
    total_wa = total_wa[:target]
    wa_count = len(total_wa)
    return total_wa, searched_acc, total_nonwa, exhausted_flag

# ====== Endpoints ======
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0, description="1 para verificar WhatsApp em lote; 0 para não verificar"),
):
    verify_bool = bool(verify)
    phones, searched_acc, non_wa_count, exhausted = _collect_and_maybe_verify(nicho, local, n, verify_bool)

    mask = None
    if verify_bool:
        # já filtramos no _collect_and_maybe_verify; marcar has_whatsapp=True
        mask = {p: True for p in phones}

    items = _json_items(phones, mask)
    return JSONResponse(
        {
            "count": len(items),
            "items": items,
            "wa_count": len(phones),
            "non_wa_count": non_wa_count,
            "searched": searched_acc,
            "exhausted": exhausted or (len(items) < n),
        }
    )

@app.get("/leads/stream")
def leads_stream(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
    verify: int = Query(0),
):
    verify_bool = bool(verify)

    def gen() -> Iterable[bytes]:
        # start
        yield b"event: start\ndata: {}\n\n"

        phones, searched_acc, non_wa_count, exhausted = _collect_and_maybe_verify(nicho, local, n, verify_bool)

        # envia itens (como streaming visual)
        for p in phones:
            payload = {"phone": p, "has_whatsapp": True if verify_bool else None}
            yield f"event: item\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")

        # progress/done
        progress = {
            "searched": searched_acc,
            "wa_count": len(phones),
            "non_wa_count": non_wa_count,
        }
        yield f"event: progress\ndata: {json.dumps(progress, ensure_ascii=False)}\n\n".encode("utf-8")

        done = {
            "count": len(phones),
            "wa_count": len(phones),
            "non_wa_count": non_wa_count,
            "searched": searched_acc,
            "exhausted": exhausted or (len(phones) < n),
        }
        yield f"event: done\ndata: {json.dumps(done, ensure_ascii=False)}\n\n".encode("utf-8")

    return StreamingResponse(gen(), media_type="text/event-stream")
