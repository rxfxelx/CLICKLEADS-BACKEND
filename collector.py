import os
import re
import time
import random
import urllib.parse
from typing import Generator, Set

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Regex BR no HTML dos resultados locais
PHONE_RE = re.compile(r"\+?(\d[\d .()\-]{8,}\d)")

# ---------- ENV / perfis ----------
BROWSER_POOL = [e.strip() for e in os.getenv("BROWSER_POOL", "chromium").split(",") if e.strip()]
PROFILE_ROOT = os.getenv("PLAYWRIGHT_PROFILE_DIR", "/app/.pw-profiles")
HEADLESS = os.getenv("HEADLESS", "1") != "0"
TZ = os.getenv("PLAYWRIGHT_TZ", "America/Sao_Paulo")

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

def _accept_consent(page):
    sels = [
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
        "div[role=button]:has-text('Aceitar tudo')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=2000)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass

def _is_blocked(page) -> bool:
    """Detecta bloqueio Google e sai sem insistir (sem tentar contornar)."""
    try:
        u = page.url or ""
        if "/sorry/" in u:
            return True
        txt = ((page.title() or "") + " " + (page.inner_text("body") or "")).lower()
        for s in ("unusual traffic", "verify you are a human", "nossos sistemas detectaram",
                  "activity from your system", "captcha"):
            if s in txt:
                return True
    except Exception:
        pass
    return False

def _numbers_from_page(page) -> Set[str]:
    found: Set[str] = set()
    # 1) links tel:
    try:
        for el in page.locator('a[href^="tel:"]').all():
            href = (el.get_attribute("href") or "")[4:]
            tel = norm_br_e164(href)
            if tel:
                found.add(tel)
    except Exception:
        pass
    # 2) regex no corpo
    try:
        body = page.inner_text("body") or ""
        for m in PHONE_RE.findall(body):
            tel = norm_br_e164(m)
            if tel:
                found.add(tel)
    except Exception:
        pass
    return found

def _pick_engine():
    return random.choice(BROWSER_POOL or ["chromium"])

def _random_ua():
    UAS = [
        # Chrome Win
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        # Firefox Linux
        "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
        # Safari Mac
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    ]
    return random.choice(UAS)

def _random_viewport():
    return {"width": random.randint(1200, 1440), "height": random.randint(800, 920)}

def _launch_persistent_context(p):
    engine = _pick_engine()
    ua = _random_ua()
    vp = _random_viewport()

    profile_dir = os.path.join(PROFILE_ROOT, f"default-{engine}")
    os.makedirs(profile_dir, exist_ok=True)

    args = ["--no-sandbox", "--disable-dev-shm-usage"]
    common_opts = dict(
        headless=HEADLESS,
        args=args,
        locale="pt-BR",
        user_agent=ua,
        viewport=vp,
        timezone_id=TZ,
    )

    if engine == "firefox":
        ctx = p.firefox.launch_persistent_context(profile_dir, **common_opts)
    elif engine == "webkit":
        ctx = p.webkit.launch_persistent_context(profile_dir, **common_opts)
    else:
        ctx = p.chromium.launch_persistent_context(profile_dir, **common_opts)

    return ctx

def iter_numbers(nicho: str, local: str, max_total: int = 200, step: int = 40) -> Generator[str, None, None]:
    """
    Gera telefones (E.164) do Google Local (tbm=lcl) de forma incremental.
    Estratégia anti-ruído:
      * contexto persistente (cookies/consent salvos),
      * engines sorteadas (chromium/firefox/webkit),
      * UA/viewport randômicos,
      * pausas com jitter,
      * sem clicar em cards (menos eventos),
      * detecção de bloqueio e saída limpa.
    """
    seen: Set[str] = set()
    produced = 0
    q = urllib.parse.quote(f"{nicho} {local}")
    start = 0
    empty_pages = 0
    last_seen_len = 0

    try:
        with sync_playwright() as p:
            ctx = _launch_persistent_context(p)
            # corta assets pesados
            ctx.route("**/*", lambda r: r.abort()
                     if r.request.resource_type in {"image", "font", "media"}
                     else r.continue_())

            page = ctx.new_page()
            page.set_default_timeout(9000)
            page.set_default_navigation_timeout(18000)

            while produced < max_total:
                url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=18000)
                except PWTimeoutError:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    start += 20
                    continue

                _accept_consent(page)

                if _is_blocked(page):
                    break

                nums = _numbers_from_page(page)
                added = 0
                for tel in nums:
                    if tel in seen:
                        continue
                    seen.add(tel)
                    produced += 1
                    added += 1
                    yield tel
                    if produced >= max_total:
                        break

                if added == 0:
                    empty_pages += 1
                else:
                    empty_pages = 0

                if last_seen_len == len(seen):
                    empty_pages += 1
                else:
                    last_seen_len = len(seen)

                if empty_pages >= 3:
                    break

                start += 20
                # jitter ajuda a não formar “assinatura” de bot
                time.sleep(0.45 + random.random() * 0.45)

            ctx.close()
    except Exception:
        return
