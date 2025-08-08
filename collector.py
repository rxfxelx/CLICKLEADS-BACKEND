# collector.py — paralelo + “fast pass” (HTML) + clique limitado
# usa persistent context e bloqueio de image/font/stylesheet/media
# Docs: launch_persistent_context; bloquear resource_type; ProcessPoolExecutor; paginação tbm=lcl (start=20). :contentReference[oaicite:0]{index=0}

import re, urllib.parse, random, math
from typing import List, Set
from concurrent.futures import ProcessPoolExecutor
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

# TUNÁVEIS
MAX_WORKERS = 3            # paralelismo (ajuste conforme RAM/CPU)
MAX_PAGES_CAP = 30         # segurança: até 30 páginas (0..580)
MAX_CLICKS_PER_PAGE = 5    # fallback: poucos cliques por página
NAV_TIMEOUT = 12000
SEL_TIMEOUT = 7000

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
                b.first.click(timeout=2500)
                page.wait_for_timeout(250)
                break
        except Exception:
            pass

def _page_fast_and_fallback(q: str, start: int, limite: int, click_limit: int) -> List[str]:
    """Coleta números de UMA página (start=0/20/40...) com fast-pass + poucos cliques."""
    out: List[str] = []; seen: Set[str] = set()
    with sync_playwright() as p:
        # persistente por página (dir único) → cookies/consent + estabilidade
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=f"/tmp/pw-data-{start}",
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
        url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
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

        # -------- FAST PASS (sem clique): card->tel: + texto do card + feed ----------
        for i in range(cards.count()):
            if len(out) >= limite: break
            try:
                el = cards.nth(i)
                # tel: no card
                tel_link = el.locator("a[href^='tel:']").first
                if tel_link.count():
                    raw = (tel_link.get_attribute("href") or "")[4:]
                    tel = norm_br_e164(raw)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel); continue
                # texto do card
                txt = el.inner_text(timeout=1200)
                for m in PHONE_RE.findall(txt or ""):
                    tel = norm_br_e164(m)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel); break
            except Exception:
                pass

        if len(out) < limite and feed.count():
            try:
                txt = feed.inner_text(timeout=1500)
                for m in PHONE_RE.findall(txt or ""):
                    tel = norm_br_e164(m)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)
                        if len(out) >= limite: break
            except Exception:
                pass

        # -------- FALLBACK (poucos cliques) ----------
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
                            if tel:
                                break
                    except Exception:
                        pass

                if tel and tel not in seen:
                    seen.add(tel); out.append(tel)

                try:
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(random.randint(160, 320))
                except Exception:
                    pass

        ctx.close()
    return out

def collect_numbers(nicho: str, local: str, limite: int = 200) -> List[str]:
    q = urllib.parse.quote(f"{nicho} {local}")
    nums: List[str] = []; seen: Set[str] = set()

    # estimativa: ~6 por página via HTML → calcula páginas necessárias
    need_pages = max(1, math.ceil(limite / 6))
    need_pages = min(need_pages, MAX_PAGES_CAP)
    offsets = [i*20 for i in range(need_pages)]  # tbm=lcl pagina 0,20,40,... :contentReference[oaicite:1]{index=1}

    # processa em lotes paralelos
    for i in range(0, len(offsets), MAX_WORKERS):
        batch = offsets[i:i+MAX_WORKERS]
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = ex.map(lambda st: _page_fast_and_fallback(q, st, limite, MAX_CLICKS_PER_PAGE), batch)
            for arr in results:
                for tel in arr:
                    if tel not in seen:
                        seen.add(tel); nums.append(tel)
                        if len(nums) >= limite:
                            return nums
    return nums
