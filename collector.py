# Rápido: fast-pass (HTML) + poucos cliques + threads, com estado e multi-cidades
import re, urllib.parse, random
from typing import List, Set, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

# TUNÁVEIS
MAX_WORKERS = 4              # threads por rodada
MAX_PAGES_CAP = 30           # páginas por cidade (0..580)
MAX_CLICKS_PER_PAGE = 4
NAV_TIMEOUT = 11000
SEL_TIMEOUT = 6500

def norm_br_e164(raw: str):
    d = re.sub(r"\D", "", raw or "")
    if not d: return None
    if not d.startswith("55"): d = "55" + d.lstrip("0")
    try:
        n = phonenumbers.parse("+" + d, None)
        if phonenumbers.is_possible_number(n) and phonenumbers.is_valid_number(n):
            return phonenumbers.format_number(n, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None

def _accept_consent(page):
    for sel in ("#L2AGLb","button:has-text('Aceitar tudo')","button:has-text('Concordo')",
                "button:has-text('I agree')","div[role=button]:has-text('Aceitar tudo')"):
        try:
            b = page.locator(sel)
            if b.count():
                b.first.click(timeout=2200)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass

def _page_fast_and_fallback(query_str: str, start: int, limite: int, click_limit: int) -> List[str]:
    out: List[str] = []; seen: Set[str] = set()
    with sync_playwright() as p:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=f"/tmp/pw-data-{abs(hash((query_str, start)))%999999}",
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage"],
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
        ctx.route("**/*", lambda r: r.abort()
                  if r.request.resource_type in {"image","font","stylesheet","media"}
                  else r.continue_())
        ctx.set_default_timeout(SEL_TIMEOUT)
        ctx.set_default_navigation_timeout(NAV_TIMEOUT)

        page = ctx.new_page()
        url = f"https://www.google.com/search?tbm=lcl&q={query_str}&hl=pt-BR&gl=BR&start={start}"
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except PWTimeoutError:
            ctx.close(); return out

        _accept_consent(page)
        try:
            page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=SEL_TIMEOUT)
        except PWTimeoutError:
            ctx.close(); return out

        feed = page.locator("div[role='feed']")
        cards = page.get_by_role("article")
        if cards.count() == 0:
            cards = page.locator("div.VkpGBb, div[role='article']")

        # FAST
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

        # Feed inteiro
        if len(out) < limite and feed.count():
            try:
                txt = feed.inner_text(timeout=1200)
                for m in PHONE_RE.findall(txt or ""):
                    tel = norm_br_e164(m)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)
                        if len(out) >= limite: break
            except Exception:
                pass

        # Fallback com poucos cliques
        if len(out) < limite:
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

# ----------------- Estado e batch por cidades -----------------
def init_state(local: str) -> Dict:
    cities = [c.strip() for c in local.split(",") if c.strip()] or [local.strip()]
    return {"cities": cities, "next_page": {c: 0 for c in cities}}

def collect_batch(nicho: str, state: Dict, limit: int) -> tuple[list[str], Dict, bool]:
    """
    Busca um LOTE de páginas (round-robin entre cidades) e retorna (nums, new_state, exhausted_all)
    """
    cities = state["cities"]
    next_page = state["next_page"].copy()

    # monta lista de páginas a processar nesta rodada (até MAX_WORKERS páginas no total)
    jobs: list[tuple[str, int]] = []
    city_idx = 0
    while len(jobs) < MAX_WORKERS:
        c = cities[city_idx % len(cities)]
        if next_page[c] >= MAX_PAGES_CAP:
            city_idx += 1
            # todas esgotadas?
            if all(next_page[x] >= MAX_PAGES_CAP for x in cities):
                break
            continue
        jobs.append((c, next_page[c]))
        next_page[c] += 1
        city_idx += 1

        # se todas já marcaram uma página nesta rodada, pode continuar round-robin

        if all(next_page[x] > state["next_page"][x] for x in cities) and len(jobs) >= MAX_WORKERS:
            break

    if not jobs:
        # esgotado
        return [], {"cities": cities, "next_page": next_page}, True

    # executa páginas em paralelo
    results: list[str] = []
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

    # limita o lote ao 'limit' pedido (apenas para reduzir explosão de dados por rodada)
    if len(results) > limit:
        results = results[:limit]

    exhausted_all = all(next_page[c] >= MAX_PAGES_CAP for c in cities)

    new_state = {"cities": cities, "next_page": next_page}
    return results, new_state, exhausted_all
