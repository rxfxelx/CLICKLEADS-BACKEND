import re
import time
import urllib.parse
from typing import List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Regex BR flexível
PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

def norm_br_e164(raw: str):
    d = re.sub(r"\D", "", raw or "")
    if not d:
        return None
    if not d.startswith("55"):
        d = "55" + d.lstrip("0")
    try:
        n = phonenumbers.parse("+" + d, None)
        if phonenumbers.is_possible_number(n) and phonenumbers.is_valid_number(n):
            return phonenumbers.format_number(n, phonenumbers.PhoneNumberFormat.E164)
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

def collect_numbers(nicho: str, local: str, alvo: int, overscan_mult: int = 8) -> Tuple[List[str], bool]:
    """
    Coleta números candidatos (E.164) no Google Local.
    Retorna (lista_candidatos, exhausted_all)
    - overscan_mult: quanto acima do alvo tentar coletar (para compensar filtro WA).
    """
    target_pool = max(alvo * overscan_mult, alvo)
    out: List[str] = []
    seen: Set[str] = set()
    exhausted_all = True

    cidades = [c.strip() for c in (local or "").split(",") if c.strip()]
    if not cidades:
        cidades = [local.strip()]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            )
            # bloquear assets pesados
            ctx.route("**/*", lambda r: r.abort()
                     if r.request.resource_type in {"image", "font", "media"}
                     else r.continue_())
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()

            for cidade in cidades:
                start = 0
                no_new_in_a_row = 0
                while len(out) < target_pool:
                    q = urllib.parse.quote(f"{nicho} {cidade}")
                    url = f"https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=18000)
                    except PWTimeoutError:
                        break
                    if _is_block(page):
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

                    # paginação e break conditions
                    added = len(seen) - before
                    if added == 0:
                        no_new_in_a_row += 1
                    else:
                        no_new_in_a_row = 0
                    if no_new_in_a_row >= 2:
                        break

                    start += 20
                    time.sleep(0.5)

                if len(out) >= target_pool:
                    break

            ctx.close()
            browser.close()
    except Exception:
        return out, False

    return out, exhausted_all
