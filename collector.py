import os
import re
import time
import urllib.parse
from typing import List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ----------------------------
# Config via ENV (com defaults)
# ----------------------------
COLLECT_STEP = max(10, int(os.getenv("COLLECT_STEP", "20")))      # paginação do Google (múltiplos de 10/20)
OVERSCAN_MULT_ENV = max(1, int(os.getenv("OVERSCAN_MULT", "6")))  # coleta mais que o pedido p/ compensar filtro WA
HEADLESS = os.getenv("HEADLESS", "1") == "1"
BROWSER_POOL = (os.getenv("BROWSER_POOL", "chromium") or "chromium").split(",")
PLAYWRIGHT_TZ = os.getenv("PLAYWRIGHT_TZ", "America/Sao_Paulo")
DEBUG_COLLECT = os.getenv("DEBUG_COLLECT", "0") == "1"

# ----------------------------
# Regex de telefone (Brasil)
# ----------------------------
PHONE_RE = re.compile(r"\+?(\d[\d .()\-]{8,}\d)")

def _log(msg: str) -> None:
    if DEBUG_COLLECT:
        print(f"[collector] {msg}", flush=True)

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

def _is_block(page) -> bool:
    try:
        txt = (page.title() or "") + " " + (page.inner_text("body") or "")
        txt = txt.lower()
        if "unusual traffic" in txt or "captcha" in txt or "/sorry/" in (page.url or ""):
            return True
    except Exception:
        pass
    return False

def _scrape_page_numbers(page) -> Set[str]:
    found: Set[str] = set()
    # 1) links tel:
    try:
        for el in page.locator("a[href^='tel:']").all():
            href = (el.get_attribute("href") or "")[4:]
            tel = norm_br_e164(href)
            if tel:
                found.add(tel)
    except Exception:
        pass
    # 2) regex no body
    try:
        body = page.inner_text("body") or ""
        for m in PHONE_RE.findall(body):
            tel = norm_br_e164(m)
            if tel:
                found.add(tel)
    except Exception:
        pass
    return found

def _choose_browser(play) -> str:
    # respeita BROWSER_POOL, mas prioriza chromium
    for name in BROWSER_POOL:
        name = name.strip().lower()
        if name in ("chromium", "firefox", "webkit"):
            return name
    return "chromium"

def _launch_browser(p):
    name = _choose_browser(p)
    _log(f"launching browser={name} headless={HEADLESS}")
    if name == "firefox":
        return p.firefox.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])
    if name == "webkit":
        return p.webkit.launch(headless=HEADLESS)
    # default chromium
    return p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"])

def collect_numbers(nicho: str, local: str, alvo: int, overscan_mult: int | None = None) -> Tuple[List[str], bool]:
    """
    Coleta números candidatos (E.164) no Google Local.
    Retorna (lista_candidatos, exhausted_all)
    - overscan_mult: quanto acima do alvo tentar coletar (para compensar filtro WA).
    """
    overscan = overscan_mult if overscan_mult and overscan_mult > 0 else OVERSCAN_MULT_ENV
    target_pool = max(alvo * overscan, alvo)
    out: List[str] = []
    seen: Set[str] = set()
    exhausted_all = True

    # suporta várias cidades separadas por vírgula
    cidades = [c.strip() for c in (local or "").split(",") if c.strip()]
    if not cidades:
        cidades = [local.strip()]

    try:
        with sync_playwright() as p:
            browser = _launch_browser(p)
            ctx = browser.new_context(
                locale="pt-BR",
                timezone_id=PLAYWRIGHT_TZ,
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            )
            # bloquear assets pesados
            ctx.route("**/*", lambda r: r.abort()
                     if r.request.resource_type in {"image","font","media"}
                     else r.continue_())
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()

            for cidade in cidades:
                start = 0
                no_new_in_a_row = 0
                _log(f"city='{cidade}' target_pool={target_pool}")
                while len(out) < target_pool:
                    q = urllib.parse.quote(f"{nicho} {cidade}")
                    url = f"https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=18000)
                    except PWTimeoutError:
                        _log("timeout on goto; moving next page")
                        start += COLLECT_STEP
                        continue

                    if _is_block(page):
                        _log("detected block/captcha; breaking city loop")
                        exhausted_all = False
                        break

                    before = len(seen)
                    nums = _scrape_page_numbers(page)
                    for tel in nums:
                        if tel not in seen:
                            seen.add(tel)
                            out.append(tel)
                            if len(out) >= target_pool:
                                break

                    added = len(seen) - before
                    _log(f"page start={start} added={added} total={len(out)}")

                    if added == 0:
                        no_new_in_a_row += 1
                    else:
                        no_new_in_a_row = 0

                    if no_new_in_a_row >= 2:
                        _log("no_new_in_a_row >= 2; breaking city loop")
                        break  # não está vindo mais nada novo

                    start += COLLECT_STEP
                    time.sleep(0.5)  # respiro p/ evitar bloqueio

                if len(out) >= target_pool:
                    break

            ctx.close()
            browser.close()
    except Exception as e:
        _log(f"collector exception: {e!r}")
        # em caso de erro, retorna o que tiver
        return out, False

    return out, exhausted_all
