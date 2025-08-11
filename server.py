import json
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from collector import collect_numbers, iter_numbers

app = FastAPI(title="Smart Leads API", version="1.6.0")

# CORS (libera geral; ajuste se quiser restringir ao seu domínio)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/leads")
def leads(nicho: str, local: str, n: int = Query(50, ge=1, le=500)):
    try:
        nums = collect_numbers(nicho, local, n)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"collector_error: {type(e).__name__}")
    return {"count": len(nums), "items": [{"phone": tel} for tel in nums]}

@app.get("/leads/stream")
def leads_stream(nicho: str, local: str, n: int = Query(50, ge=1, le=500)):
    def gen():
        yield f"event: start\ndata: {json.dumps({'target': n})}\n\n"
        count = 0
        try:
            for tel in iter_numbers(nicho, local, n):
                count += 1
                yield f"event: item\ndata: {json.dumps({'phone': tel, 'count': count})}\n\n"
        finally:
            yield f"event: done\ndata: {json.dumps({'count': count})}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")
