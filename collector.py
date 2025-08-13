import re
import time
import urllib.parse
from typing import Dict, List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ---- Regex / normalização ----
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

# ---- Consent do Google ----
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

# ---- Extração clicando nos cards (mais confiável para aparecer o telefone) ----
def _extract_by_clicking(page, max_clicks: int = 25) -> List[str]:
    found: List[str] = []
    seen: Set[str] = set()

    cards = page.get_by_role("article")
    if cards.count() == 0:
        cards = page.locator("div.VkpGBb, div[role='article']")

    total = min(cards.count(), max_clicks)
    for i in range(total):
        try:
            it = cards.nth(i)
            it.scroll_into_view_if_needed(timeout=2000)
            it.click(timeout=4000, force=True)

            # Espera algo de telefone no painel
            try:
                page.wait_for_selector(
                    "a[href^='tel:'], span:has-text('Telefone'), div:has-text('Telefone')",
                    timeout=6000
                )
            except PWTimeoutError:
                pass

            # a) link tel:
            try:
                link = page.locator("a[href^='tel:']").first
                if link.count():
                    raw = (link.get_attribute("href") or "")[4:]
                    tel = norm_br_e164(raw)
                    if tel and tel not in seen:
                        seen.add(tel); found.append(tel)
            except Exception:
                pass

            # b) regex no body
            try:
                blob = page.inner_text("body")
                for m in PHONE_RE.findall(blob or ""):
                    tel = norm_br_e164(m)
                    if tel and tel not in seen:
                        seen.add(tel); found.append(tel)
            except Exception:
                pass

            # fecha painel
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass

            # pausa curta estável
            page.wait_for_timeout(200)
        except Exception:
            continue

    return found

# ---- Coleta por cidade / lote ----
def collect_numbers_for_city(nicho: str, city: str, limit: int, start: int) -> Tuple[List[str], int, int, bool]:
    """
    Coleta até 'limit' números numa cidade, a partir de 'start' (0,20,40...).
    Estratégia direta: abre página, clica nos cards e extrai.
    """
    phones: List[str] = []
    seen: Set[str] = set()
    searched = 0
    exhausted_city = False

    q = urllib.parse.quote(f"{nicho} {city}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
        # bloqueia imagens/fonts para acelerar
        ctx.route("**/*", lambda r: r.abort() if r.request.resource_type in {"image","font","media"} else r.continue_())
        ctx.set_default_timeout(10000)
        ctx.set_default_navigation_timeout(20000)

        page = ctx.new_page()
        cur = start
        empty_pages = 0
        last_len = -1

        while len(phones) < limit:
            url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={cur}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
            except PWTimeoutError:
                empty_pages += 1
                if empty_pages >= 2:
                    exhausted_city = True
                    break
                cur += 20
                continue

            _accept_consent(page)

            # garante que a lista apareceu
            try:
                page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=7000)
            except PWTimeoutError:
                empty_pages += 1
                if empty_pages >= 2:
                    exhausted_city = True
                    break
                cur += 20
                continue

            # clica e extrai
            got = _extract_by_clicking(page, max_clicks=28)
            searched += len(got)
            for t in got:
                if t not in seen:
                    seen.add(t); phones.append(t)
                    if len(phones) >= limit:
                        break

            # estagnação/termino
            if last_len == len(phones):
                empty_pages += 1
            else:
                last_len = len(phones)
                empty_pages = 0

            if empty_pages >= 3:
                exhausted_city = True
                break

            cur += 20
            page.wait_for_timeout(300)  # cadência estável

        ctx.close()
        browser.close()

    return phones, searched, cur, exhausted_city

def collect_numbers_batch(
    nicho: str,
    cities: List[str],
    limit: int,
    start_by_city: Dict[str, int] | None = None
) -> Tuple[List[str], int, Dict[str, int], bool]:
    """
    Percorre as cidades na ordem, mantendo offsets, até juntar 'limit'.
    Retorna (phones, searched_total, next_start_by_city, exhausted_all)
    """
    if start_by_city is None:
        start_by_city = {c: 0 for c in cities}

    result: List[str] = []
    searched_total = 0
    exhausted_flags: Dict[str, bool] = {c: False for c in cities}

    for city in cities:
        if len(result) >= limit:
            break
        if exhausted_flags.get(city, False):
            continue

        start = start_by_city.get(city, 0)
        remaining = max(0, limit - len(result))
        phones, searched, next_start, exhausted_city = collect_numbers_for_city(nicho, city, remaining, start)

        for t in phones:
            if t not in result:
                result.append(t)
                if len(result) >= limit:
                    break

        searched_total += searched
        start_by_city[city] = next_start
        exhausted_flags[city] = exhausted_city

    exhausted_all = all(exhausted_flags.get(c, False) for c in cities)
    return result, searched_total, start_by_city, exhausted_all
