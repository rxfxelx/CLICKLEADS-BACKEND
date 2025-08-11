# collector.py — v2.2.0
# - Browser singleton (sem corrida de event loop)
# - NÃO bloqueia stylesheet (apenas image/font/media)
# - Fallbacks extras: page.content() e body text
# - Round-robin multi-cidades + threads com contexts

import re, urllib.parse, random
from typing import List, Set, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# ---------------- Configs ----------------
PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

MAX_WORKERS = 4               # páginas em paralelo (1 por thread)
MAX_PAGES_CAP = 30            # páginas por cidade (0,20,40,...)
MAX_CLICKS_PER_PAGE = 4       # cliques de fallback por página
NAV_TIMEOUT = 11000           # ms
SEL_TIMEOUT = 6500            # ms

# ------------- Playwright singleton -------------
_PW = None
_BROWSER = None
_INIT_LOCK = threading.Lock()

def _ensure_browser():
    """Garante UMA instância global de Playwright + Browser (thread-safe)."""
    global _PW, _BROWSER
    if _BROWSER:
        return _BROWSER
    with _INIT_LOCK:
        if _BROWSER:
            return _BROWSER
        _PW = sync_playwright().start()
        _BROWSER = _PW.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
    return _BROWSER

# ------------- Utilidades -------------
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

def _accept_consent(page):
    for sel in (
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
        "div[role=button]:has-text('Aceitar tudo')",
    ):
        try:
            b = page.locator(sel)
            if b.count():
                b.first.click(timeout=2200)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass

# ------------- Coleta de UMA página -------------
def _page_fast_and_fallback(query_str: str, start: int, limite: int, click_limit: int) -> List[str]:
    """
    Coleta números de UMA página (start=0/20/40...) do Google Local Finder.
    Estratégia:
      1) regex no HTML completo
      2) fast pass nos cards (link tel: / texto)
      3) regex no feed
      4) regex no body
      5) fallback com poucos cliques em cards (painel)
    """
    out: List[str] = []
    seen: Set[str] = set()

    browser = _ensure_browser()
    ctx = browser.new_context(
        locale="pt-BR",
        user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    )
    # Bloquear somente recursos pesados (NÃO bloquear stylesheet)
    ctx.route(
        "**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media"}
        else route.continue_(),
    )
    ctx.set_default_timeout(SEL_TIMEOUT)
    ctx.set_default_navigation_timeout(NAV_TIMEOUT)

    page = ctx.new_page()
    url = f"https://www.google.com/search?tbm=lcl&q={query_str}&hl=pt-BR&gl=BR&start={start}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        # dá um respiro pro Local Finder montar
        page.wait_for_selector("body", timeout=3000)
        page.wait_for_timeout(600)
    except PWTimeoutError:
        ctx.close()
        return out

    _accept_consent(page)

    # (0) Fallback super-rápido: regex no HTML completo
    try:
        html = page.content()
        for m in PHONE_RE.findall(html or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel)
                out.append(tel)
                if len(out) >= limite:
                    break
    except Exception:
        pass
    if len(out) >= limite:
        ctx.close()
        return out

    # Garante que pelo menos algo do Local Finder existe
    try:
        page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=SEL_TIMEOUT)
    except PWTimeoutError:
        # (1b) se não montou, tenta regex no body como último recurso
        try:
            body_txt = page.inner_text("body")
            for m in PHONE_RE.findall(body_txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel)
                    out.append(tel)
                    if len(out) >= limite:
                        break
        except Exception:
            pass
        ctx.close()
        return out

    feed = page.locator("div[role='feed']")
    cards = page.get_by_role("article")
    if cards.count() == 0:
        cards = page.locator("div.VkpGBb, div[role='article']")

    # (1) FAST: varre cards sem clicar
    for i in range(cards.count()):
        if len(out) >= limite:
            break
        try:
            el = cards.nth(i)
            tel_link = el.locator("a[href^='tel:']").first
            if tel_link.count():
                raw = (tel_link.get_attribute("href") or "")[4:]
                tel = norm_br_e164(raw)
                if tel and tel not in seen:
                    seen.add(tel)
                    out.append(tel)
                    continue
            txt = el.inner_text(timeout=900)
            for m in PHONE_RE.findall(txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel)
                    out.append(tel)
                    break
        except Exception:
            pass

    if len(out) >= limite:
        ctx.close()
        return out

    # (2) regex no feed inteiro
    if feed.count():
        try:
            txt = feed.inner_text(timeout=1200)
            for m in PHONE_RE.findall(txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel)
                    out.append(tel)
                    if len(out) >= limite:
                        break
        except Exception:
            pass

    if len(out) >= limite:
        ctx.close()
        return out

    # (3) regex no body como fallback geral
    try:
        body_txt = page.inner_text("body")
        for m in PHONE_RE.findall(body_txt or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel)
                out.append(tel)
                if len(out) >= limite:
                    break
    except Exception:
        pass

    if len(out) >= limite:
        ctx.close()
        return out

    # (4) Poucos cliques para tentar abrir painel e extrair telefone
    to_click = min(click_limit, cards.count(), max(0, limite - len(out)))
    for i in range(to_click):
        if len(out) >= limite:
            break
        try:
            it = cards.nth(i)
            it.scroll_into_view_if_needed()
            it.click(force=True)
            page.wait_for_selector(
                "a[href^='tel:'], span:has-text('Telefone'), div:has-text('Telefone')",
                timeout=SEL_TIMEOUT,
            )
        except PWTimeoutError:
            continue

        tel = None
        try:
            link = page.locator("a[href^='tel:']").first
            if link.count():
                raw = (link.get_attribute("href") or "")[4:]
                tel = norm_br_e164(raw)
        except PWError:
            tel = None

        if not tel:
            try:
                panel = page.locator("div[role='dialog'], div[role='region'], div[aria-modal='true']")
                blob = panel.inner_text() if panel.count() else ""
                for m in PHONE_RE.findall(blob or ""):
                    tel = norm_br_e164(m)
                    if tel:
                        break
            except Exception:
                pass

        if tel and tel not in seen:
            seen.add(tel)
            out.append(tel)

        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(random.randint(120, 240))
        except Exception:
            pass

    ctx.close()
    return out

# ------------- Estado / Batch por cidades -------------
def init_state(local: str) -> Dict:
    cities = [c.strip() for c in local.split(",") if c.strip()] or [local.strip()]
    return {"cities": cities, "next_page": {c: 0 for c in cities}}

def collect_batch(nicho: str, state: Dict, limit: int) -> Tuple[List[str], Dict, bool]:
    """
    Busca um LOTE de páginas (round-robin entre cidades) e retorna (nums, new_state, exhausted_all).
    """
    # garante browser antes de spawnar threads
    _ensure_browser()

    cities = state["cities"]
    next_page = state["next_page"].copy()

    jobs: List[Tuple[str, int]] = []
    city_idx = 0
    while len(jobs) < MAX_WORKERS:
        c = cities[city_idx % len(cities)]
        if next_page[c] >= MAX_PAGES_CAP:
            city_idx += 1
            if all(next_page[x] >= MAX_PAGES_CAP for x in cities):
                break
            continue
        jobs.append((c, next_page[c]))
        next_page[c] += 1
        city_idx += 1
        if all(next_page[x] > state["next_page"][x] for x in cities) and len(jobs) >= MAX_WORKERS:
            break

    if not jobs:
        return [], {"cities": cities, "next_page": next_page}, True

    results: List[str] = []
    with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
        futs = []
        for (city, page_idx) in jobs:
            q = urllib.parse.quote(f"{nicho} {city}")
            start = page_idx * 20
            futs.append(ex.submit(_page_fast_and_fallback, q, start, limit, MAX_CLICKS_PER_PAGE))
        for fut in as_completed(futs):
            try:
                arr = fut.result() or []
                results.extend(arr)
            except Exception:
                pass

    if len(results) > limit:
        results = results[:limit]

    exhausted_all = all(next_page[c] >= MAX_PAGES_CAP for c in cities)
    new_state = {"cities": cities, "next_page": next_page}
    return results, new_state, exhausted_all

# ------------- APIs usadas pelo server -------------
def iter_numbers(nicho: str, local: str, limite: int = 50):
    cities = [c.strip() for c in local.split(",") if c.strip()] or [local.strip()]
    total = 0
    for cidade in cities:
        rem = max(1, limite - total)
        state = {"cities": [cidade], "next_page": {cidade: 0}}
        while total < limite:
            batch, state, exhausted = collect_batch(nicho, state, rem)
            for tel in batch:
                yield tel
                total += 1
                if total >= limite:
                    return
            if exhausted:
                break

def collect_numbers(nicho: str, local: str, limite: int = 50) -> List[str]:
    out: List[str] = []
    for tel in iter_numbers(nicho, local, limite):
        out.append(tel)
        if len(out) >= limite:
            break
    return out

def collect_numbers_info(nicho: str, local: str, limite: int = 50) -> Tuple[List[str], bool]:
    nums: List[str] = []
    exhausted_all = False
    cities = [c.strip() for c in local.split(",") if c.strip()] or [local.strip()]
    for cidade in cities:
        rem = max(1, limite - len(nums))
        state = {"cities": [cidade], "next_page": {cidade: 0}}
        while len(nums) < limite:
            batch, state, exhausted = collect_batch(nicho, state, rem)
            for t in batch:
                if t not in nums:
                    nums.append(t)
            if len(nums) >= limite or exhausted:
                exhausted_all = exhausted_all or exhausted
                break
    return nums[:limite], exhausted_all or (len(nums) < limite)
