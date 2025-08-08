# Backend — FastAPI + Playwright (Railway-ready)

## Deploy
1. Suba estes arquivos em um serviço no Railway.
2. Use **Dockerfile** para build.
3. Defina envs (Variables):
   - `UAZAPI_CHECK_URL` = https://helsenia.uazapi.com/chat/check
   - `UAZAPI_ADMIN_TOKEN` = <seu token>

## Testes
- `GET /health` -> {"ok": true}
- `GET /leads?nicho=médico&local=Belo Horizonte&n=20`

Imagem base Playwright já traz Chromium/headless.
