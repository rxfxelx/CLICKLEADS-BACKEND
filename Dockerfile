FROM mcr.microsoft.com/playwright/python:v1.53.0-noble

WORKDIR /app

# deps Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# **garante** que o Chromium e as libs estejam instalados (idempotente nesse base image)
RUN python -m playwright install --with-deps chromium

# app
COPY . .

# envs úteis
ENV PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Railway injeta $PORT; local usa 8080
CMD bash -lc 'uvicorn server:app --host 0.0.0.0 --port ${PORT:-8080}'
