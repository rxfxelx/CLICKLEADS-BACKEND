# collector.py — v2.2.1 (hotfix)
# - MAX_WORKERS=1 (evita concorrência: Playwright sync não é thread-safe)
# - Reinício automático do browser se perder a conexão
# - NÃO bloqueia stylesheet (só image/font/media)
# - Fallbacks: page.content() → feed → body → poucos cliques

import re, urllib.parse, random, threading
from typing import List, Set, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

# ---- Parâmetros de execução ----
MAX_WORKERS = 1               # Playwright sync não é thread-safe -> uma thread
MAX_PAGES_CAP = 30            # páginas por cidade (0,20,40,...)
MAX_CLICKS_PER_PAGE = 4
NAV_TIMEOUT = 11000
SEL_TIMEOUT = 6500

# ---- Playwright singleton com auto-restart ----
_PW = None
_BROWSER = None
_INIT_LOCK = threading.Lock()

def _start_browser():
    global _PW, _BROWSER
    if _PW is None:
        _PW = sync_playwright().start()
    _BROWSER = _PW.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage",
              "--disable-blink-features=AutomationControlled"],
    )

def _ensure_browser():
    global _BROWSER
    with _INIT_LOCK:
        if _BROWSER is None:
            _start_browser()
        else:
            try:
                # Playwright expõe is_connected() no Browser
                if not _BROWSER.is_connected():
                    _start_browser()
            except Exception:
                _start_browser()
    return _BROWSER

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
    for sel in ("#L2AGLb",
                "button:has-text('Aceitar tudo')",
                "button:has-text('Concordo')",
                "button:has-text('I agree')",
                "div[role=button]:has-text('Aceitar tudo')"):
        try:
            b = page.locator(sel)
            if b.count():
                b.first.click(timeout=2200)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass

def _new_context_retry(browser):
    # cria um contexto e roteia recursos pesados; se falhar, reinicia browser 1x
    try:
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
    except PWError:
        # reinicia browser e tenta de novo
        with _INIT_LOCK:
            try:
                if browser and browser.is_connected():
                    browser.close()
            except Exception:
                pass
            _start_browser()
        browser = _ensure_browser()
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )

    ctx.route("**/*",
        lambda route: route.abort()
        if route.request.resource_type in {"image", "font", "media"}
        else route.continue_()
    )
    ctx.set_default_timeout(SEL_TIMEOUT)
    ctx.set_default_navigation_timeout(NAV_TIMEOUT)
    return ctx

def _page_fast_and_fallback(query_str: str, start: int, limite: int, click_limit: int) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()

    browser = _ensure_browser()
    ctx = _new_context_retry(browser)
    page = ctx.new_page()

    url = f"https://www.google.com/search?tbm=lcl&q={query_str}&hl=pt-BR&gl=BR&start={start}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        page.wait_for_selector("body", timeout=3000)
        page.wait_for_timeout(600)
    except PWTimeoutError:
        ctx.close(); return out

    _accept_consent(page)

    # 0) regex no HTML completo
    try:
        html = page.content()
        for m in PHONE_RE.findall(html or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
                if len(out) >= limite: break
    except Exception:
        pass
    if len(out) >= limite:
        ctx.close(); return out

    # garante algo do Local Finder
    try:
        page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=SEL_TIMEOUT)
    except PWTimeoutError:
        try:
            body_txt = page.inner_text("body")
            for m in PHONE_RE.findall(body_txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel); out.append(tel)
                    if len(out) >= limite: break
        except Exception:
            pass
        ctx.close(); return out

    feed = page.locator("div[role='feed']")
    cards = page.get_by_role("article")
    if cards.count() == 0:
        cards = page.locator("div.VkpGBb, div[role='article']")

    # 1) varre cards sem clicar
    for i in range(cards.count()):
        if len(out) >= limite: break
        try:
            el = cards.nth(i)
            tel_link = el.locator("a[href^='tel:']").first
            if tel_link.count():
                raw = (tel_link.get_attribute("href") or "")[4:]
                tel = norm_br_e164(raw)
                if tel and tel not in seen:
                    seen.add(tel); out.append(tel); continue
            txt = el.inner_text(timeout=900)
            for m in PHONE_RE.findall(txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel); out.append(tel); break
        except Exception:
            pass
    if len(out) >= limite:
        ctx.close(); return out

    # 2) regex no feed
    if feed.count():
        try:
            txt = feed.inner_text(timeout=1200)
            for m in PHONE_RE.findall(txt or ""):
                tel = norm_br_e164(m)
                if tel and tel not in seen:
                    seen.add(tel); out.append(tel)
                    if len(out) >= limite: break
        except Exception:
            pass
    if len(out) >= limite:
        ctx.close(); return out

    # 3) regex no body
    try:
        body_txt = page.inner_text("body")
        for m in PHONE_RE.findall(body_txt or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
                if len(out) >= limite: break
    except Exception:
        pass
    if len(out) >= limite:
        ctx.close(); return out

    # 4) poucos cliques em cards
    to_click = min(click_limit, cards.count(), max(0, limite - len(out)))
    for i in range(to_click):
        if len(out) >= limite: break
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
                    if tel: break
            except Exception:
                pass

        if tel and tel not in seen:
            seen.add(tel); out.append(tel)

        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(random.randint(120, 240))
        except Exception:
            pass

    ctx.close()
    return out

# ---- Round-robin por cidades (sem concorrência) ----
def init_state(local: str) -> Dict:
    cities = [c.strip() for c in local.split(",") if c.strip()] or [local.strip()]
    return {"cities": cities, "next_page": {c: 0 for c in cities}}

def collect_batch(nicho: str, state: Dict, limit: int) -> Tuple[List[str], Dict, bool]:
    _ensure_browser()  # aquece
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

    if not jobs:
        return [], {"cities": cities, "next_page": next_page}, True

    results: List[str] = []
    # ainda usamos ThreadPool, mas com max_workers=1 (sem concorrência real)
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
