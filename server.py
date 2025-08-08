# server.py — sem verificação de WhatsApp (só retorna os leads)

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from collector import collect_numbers

app = FastAPI(title="Lead Extractor API", version="1.3.0")

# CORS: libera produção e previews do Vercel
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https://.*\.vercel\.app$",
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
def leads(
    nicho: str = Query(...),
    local: str = Query(...),
    n: int = Query(50, ge=1, le=500),
):
    try:
        nums = collect_numbers(nicho, local, n)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")

    return {"count": len(nums), "items": [{"phone": tel} for tel in nums]}
