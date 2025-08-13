import os
import re
import time
import urllib.parse
from typing import List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# -----------------------------
# ENV
# -----------------------------
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

COLLECT_STEP        = _env_int("COLLECT_STEP", 40)     # incremento do start= (20 por página; usar 40 evita ficar preso)
HEADLESS            = os.getenv("HEADLESS", "1").strip() not in {"0", "false", "False"}
BROWSER_POOL        = [b.strip() for b in os.getenv("BROWSER_POOL", "chromium,firefox,webkit").split(",") if b.strip()]
PLAYWRIGHT_PROFILE_DIR = os.getenv("PLAYWRIGHT_PROFILE_DIR", "").strip()  # ex: /app/.pw-profiles
PLAYWRIGHT_TZ       = os.getenv("PLAYWRIGHT_TZ", "America/Sao_Paulo").strip()

# -----------------------------
# Telefones
# -----------------------------
PHONE_RE = re.compile(r"\+?(\d[\d .()\-]{8,}\d)")

def norm_br_e164(raw: str) -> str | None:
    d = re.sub(r"\D", "", raw or "")
    if not d:
        return None
    if not d.startswith("55"):
        d = "55" + d.lstrip("0")
    try:
        num = phonenumbers.parse("+" + d, None)
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None

# -----------------------------
# Anti-bloqueio simples
# -----------------------------
def _is_block(page) -> bool:
    try:
        txt = ((page.title() or "") + " " + (page.inner_text("body") or "")).lower()
        if "unusual traffic" in txt or "captcha" in txt or "/sorry/" in (page.url or ""):
            return True
    except Exception:
        pass
    return False

def _accept_consent(page):
    sels = [
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
        "div[role=button]:has-text('Aceitar tudo')",
        "button:has-text('Aceitar')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=1500)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass

# -----------------------------
# Extração
# -----------------------------
def _numbers_from_page(page) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    # 1) links tel:
    try:
        for a in page.locator('a[href^="tel:"]').all():
            raw = (a.get_attribute("href") or "")[4:]
            tel = norm_br_e164(raw)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
    except Exception:
        pass

    # 2) Regex no body (cartões + painel/sumário)
    try:
        body = page.inner_text("body") or ""
        for m in PHONE_RE.findall(body):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
    except Exception:
        pass

    return out

def _open_browser(p, engine: str):
    if engine == "firefox":
        return p.firefox.launch(headless=HEADLESS)
    if engine == "webkit":
        return p.webkit.launch(headless=HEADLESS)
    return p.chromium.launch(headless=HEADLESS, args=[
        "--no-sandbox", "--disable-dev-shm-usage"
    ])

def _new_context(browser, engine: str):
    ctx_kwargs = dict(
        locale="pt-BR",
        timezone_id=PLAYWRIGHT_TZ,
        user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    )
    if PLAYWRIGHT_PROFILE_DIR:
        # contexto persistente por engine
        return browser.new_context(**ctx_kwargs, record_video_dir=None)
    return browser.new_context(**ctx_kwargs)

def collect_numbers(nicho: str, local: str, alvo: int, overscan_mult: int = 8) -> Tuple[List[str], bool]:
    """
    Coleta números candidatos (E.164) no Google Local com fallback.
    Retorna (lista_candidatos, exhausted_all)
    - overscan_mult: coletar acima do alvo p/ compensar filtro WA.
    """
    target_pool = max(alvo * overscan_mult, alvo)
    out: List[str] = []
    seen: Set[str] = set()
    exhausted_all = True

    cities = [c.strip() for c in (local or "").split(",") if c.strip()]
    if not cities:
        cities = [local.strip()]

    try:
        with sync_playwright() as p:
            # revezar engines ajuda a variar fingerprint
            engines = BROWSER_POOL or ["chromium"]
            eng_idx = 0

            for city in cities:
                if len(out) >= target_pool:
                    break

                # troca de engine por cidade — leve variação
                engine = engines[eng_idx % len(engines)]
                eng_idx += 1
                browser = _open_browser(p, engine)
                ctx = _new_context(browser, engine)

                # bloqueia assets pesados
                ctx.route("**/*", lambda r: r.abort()
                          if r.request.resource_type in {"image", "font", "media"}
                          else r.continue_())
                ctx.set_default_timeout(9000)
                ctx.set_default_navigation_timeout(18000)
                page = ctx.new_page()

                start = 0
                empty_pages = 0
                last_total = 0

                # -------- 1) tbm=lcl (páginas locais) --------
                while len(out) < target_pool:
                    q = urllib.parse.quote(f"{nicho} {city}")
                    url = f"https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=18000)
                    except PWTimeoutError:
                        empty_pages += 1
                        if empty_pages >= 2:
                            break
                        start += COLLECT_STEP
                        continue

                    _accept_consent(page)
                    if _is_block(page):
                        exhausted_all = False
                        break

                    try:
                        page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=6000)
                    except PWTimeoutError:
                        pass

                    nums = _numbers_from_page(page)
                    for tel in nums:
                        if tel not in seen:
                            seen.add(tel)
                            out.append(tel)
                            if len(out) >= target_pool:
                                break

                    if len(out) == last_total:
                        empty_pages += 1
                    else:
                        empty_pages = 0
                        last_total = len(out)

                    if empty_pages >= 3:
                        break

                    start += COLLECT_STEP
                    page.wait_for_timeout(600)

                # -------- 2) fallback: busca “normal” com palavra telefone --------
                if len(out) < target_pool:
                    try:
                        qf = urllib.parse.quote(f"{nicho} {city} telefone")
                        urlf = f"https://www.google.com/search?hl=pt-BR&gl=BR&q={qf}"
                        page.goto(urlf, wait_until="domcontentloaded", timeout=18000)
                        _accept_consent(page)
                        if not _is_block(page):
                            nums = _numbers_from_page(page)
                            for tel in nums:
                                if tel not in seen:
                                    seen.add(tel)
                                    out.append(tel)
                                    if len(out) >= target_pool:
                                        break
                    except Exception:
                        pass

                ctx.close()
                browser.close()

                # Se a cidade rendeu pouco, considerar que não está esgotado (pode ter sido block)
                if len(out) < target_pool:
                    exhausted_all = False

                # limite atingido
                if len(out) >= target_pool:
                    break

    except Exception:
        # retorna o que deu
        return out, False

    return out, exhausted_all
