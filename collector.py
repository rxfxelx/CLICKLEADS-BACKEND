import re
import time
import urllib.parse
from typing import Dict, List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Regex de telefone (Brasil) — robusto para card/aria/innerText
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

def _accept_consent(page):
    # Alguns diálogos do Google
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

def _numbers_from_local_page(page) -> List[str]:
    """
    Extrai números usando:
     1) a[href^='tel:']
     2) Regex no texto dos cartões e painel lateral
    """
    out: List[str] = []
    seen: Set[str] = set()

    # 1) links tel:
    for a in page.locator('a[href^="tel:"]').all():
        try:
            raw = (a.get_attribute("href") or "").replace("tel:", "")
            tel = norm_br_e164(raw)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
        except Exception:
            continue

    # 2) Regex no body (cartões + painel)
    try:
        blob = page.inner_text("body")
        for m in PHONE_RE.findall(blob or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
    except Exception:
        pass

    return out

def collect_numbers_for_city(nicho: str, city: str, limit: int, start: int) -> Tuple[List[str], int, int, bool]:
    """
    Coleta 'limit' números para uma cidade, a partir de 'start' (paginação 0,20,40...).
    Retorna (phones, searched, next_start, exhausted_city).
    """
    phones: List[str] = []
    searched = 0
    exhausted_city = False

    q = urllib.parse.quote(f"{nicho} {city}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
        )
        # Bloqueia assets pesados
        ctx.route("**/*", lambda r: r.abort() if r.request.resource_type in {"image","font","media"} else r.continue_())
        ctx.set_default_timeout(9000)
        ctx.set_default_navigation_timeout(18000)

        page = ctx.new_page()

        cur = start
        # Guarda o último length para detectar "loop sem resultado"
        last_count = -1
        empty_pages = 0

        while len(phones) < limit:
            url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={cur}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=18000)
            except PWTimeoutError:
                empty_pages += 1
                if empty_pages >= 2:
                    exhausted_city = True
                    break
                cur += 20
                continue

            _accept_consent(page)

            # aguarda render de algum card
            had_cards = True
            try:
                page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=7000)
            except PWTimeoutError:
                had_cards = False

            new_nums = _numbers_from_local_page(page)
            searched += len(new_nums)

            # adiciona ao lot
            for t in new_nums:
                if t not in phones:
                    phones.append(t)
                    if len(phones) >= limit:
                        break

            if not had_cards:
                empty_pages += 1
            else:
                empty_pages = 0

            # detecta estagnação (sem novos)
            if last_count == len(phones):
                empty_pages += 1
            else:
                last_count = len(phones)

            # se insistimos e não vem nada, marcar esgotado
            if empty_pages >= 3:
                exhausted_city = True
                break

            cur += 20
            # pequeno respiro para evitar recaptcha
            time.sleep(0.6)

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
    Coleta até 'limit' números percorrendo as cidades na ordem recebida.
    Mantém/atualiza offsets por cidade.
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
        if exhausted_flags.get(city) is True:
            continue

        start = start_by_city.get(city, 0)
        remaining = max(0, limit - len(result))
        phones, searched, next_start, exhausted_city = collect_numbers_for_city(nicho, city, remaining, start)

        # agrega mantendo ordem e sem duplicar
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
