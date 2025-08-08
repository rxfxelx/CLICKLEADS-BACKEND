FROM mcr.microsoft.com/playwright/python:v1.53.0-noble
WORKDIR /app

# deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app
COPY . .

ENV PYTHONUNBUFFERED=1
# Railway injects $PORT. Default to 8080 if local.
CMD bash -lc 'uvicorn server:app --host 0.0.0.0 --port ${PORT:-8080}'
