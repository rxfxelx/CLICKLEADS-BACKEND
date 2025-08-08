\
import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
from collector import collect_numbers

# UAZAPI envs
UAZAPI_CHECK_URL = os.getenv("UAZAPI_CHECK_URL", "").rstrip("/")  # ex: https://helsenia.uazapi.com/chat/check
UAZAPI_ADMIN_TOKEN = os.getenv("UAZAPI_ADMIN_TOKEN")              # admin token

app = FastAPI(title="Lead Extractor API", version="1.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

async def check_whatsapp(e164: str):
    if not (UAZAPI_CHECK_URL and UAZAPI_ADMIN_TOKEN):
        return None

    # tenta Authorization: Bearer e apikey, com 3 formatos de body
    headers_list = [
        {"Authorization": f"Bearer {UAZAPI_ADMIN_TOKEN}", "Content-Type": "application/json"},
        {"apikey": UAZAPI_ADMIN_TOKEN, "Content-Type": "application/json"},
    ]
    bodies = [
        {"number": e164},
        {"numbers": [e164]},
        {"phone": e164},
    ]

    async with httpx.AsyncClient(timeout=20) as cx:
        for headers in headers_list:
            for body in bodies:
                try:
                    r = await cx.post(UAZAPI_CHECK_URL, json=body, headers=headers)
                    if 200 <= r.status_code < 300:
                        data = r.json()
                        if isinstance(data, dict):
                            if any(k in data for k in ("exists", "valid", "is_whatsapp")):
                                return bool(data.get("exists") or data.get("valid") or data.get("is_whatsapp"))
                            arr = data.get("data") or data.get("numbers")
                            if isinstance(arr, list) and arr:
                                item = arr[0]
                                if isinstance(item, dict):
                                    return bool(item.get("exists") or item.get("valid") or item.get("is_whatsapp"))
                except Exception:
                    continue
    return None

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/leads")
async def leads(nicho: str = Query(...), local: str = Query(...), n: int = Query(50, ge=1, le=500)):
    nums = collect_numbers(nicho, local, n)
    out = []
    for tel in nums:
        is_wa = await check_whatsapp(tel)
        out.append({"phone": tel, "is_whatsapp": is_wa})
    return {"count": len(out), "items": out}
