import os, re, time, random, urllib.parse
from typing import Dict, List, Set, Tuple
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

DEBUG = os.getenv("DEBUG", "0") == "1"
def _dbg(*a): 
    if DEBUG: print("[collector]", *a, flush=True)

PHONE_RE = re.compile(r"\+?(\d[\d .()\-]{8,}\d)")
UAS = [
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
]

def norm_br_e164(raw: str) -> str|None:
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
    sels = [
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
        "div[role=button]:has-text('Aceitar tudo')",
    ]
    # main
    for sel in sels:
        try:
            el = page.locator(sel)
            if el.count():
                el.first.click(timeout=2000)
                page.wait_for_timeout(200)
                return
        except Exception:
            pass
    # iframes
    try:
        for f in page.frames:
            for sel in sels:
                try:
                    el = f.locator(sel)
                    if el.count():
                        el.first.click(timeout=2000)
                        page.wait_for_timeout(200)
                        return
                except Exception:
                    continue
    except Exception:
        pass

def _numbers_from_page(page) -> List[str]:
    out: List[str] = []; seen: Set[str] = set()
    # tel: links
    try:
        for a in page.locator('a[href^="tel:"]').all():
            raw = (a.get_attribute("href") or "")[4:]
            tel = norm_br_e164(raw)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
    except Exception:
        pass
    # texto
    try:
        body = page.inner_text("body")
        for m in PHONE_RE.findall(body or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel); out.append(tel)
    except Exception:
        pass
    return out

def _open_ctx(p):
    ua = random.choice(UAS)
    ctx = p.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"]
    ).new_context(
        locale="pt-BR",
        user_agent=ua,
        timezone_id="America/Sao_Paulo",
    )
    # bloqueia assets pesados
    ctx.route("**/*", lambda r: r.abort() if r.request.resource_type in {"image","font","media"} else r.continue_())
    ctx.set_default_timeout(9000)
    ctx.set_default_navigation_timeout(18000)
    return ctx

def collect_numbers_for_city(nicho: str, city: str, want: int, start: int) -> Tuple[List[str], int, int, bool]:
    """Varre o Local Finder daquela cidade, a partir de start (0,20,40...)."""
    phones: List[str] = []; searched = 0; exhausted = False
    q = urllib.parse.quote(f"{nicho} {city}")
    _dbg(f"city={city} want={want} start={start}")

    with sync_playwright() as p:
        ctx = _open_ctx(p)
        page = ctx.new_page()
        cur = start
        empty_streak = 0
        last_len = -1

        while len(phones) < want:
            url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={cur}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=18000)
            except PWTimeoutError:
                empty_streak += 1
                if empty_streak >= 2: exhausted = True; break
                cur += 20; continue

            _accept_consent(page)
            try:
                page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=7000)
            except PWTimeoutError:
                pass

            nums = _numbers_from_page(page)
            searched += len(nums)
            for t in nums:
                if t not in phones:
                    phones.append(t)
                    if len(phones) >= want: break

            if len(phones) == last_len:
                empty_streak += 1
            else:
                empty_streak = 0
                last_len = len(phones)

            if empty_streak >= 3: exhausted = True; break
            cur += 20
            page.wait_for_timeout(400 + random.randint(60,160))  # respira

        ctx.close()
    _dbg(f"city={city} got={len(phones)} next={cur} exhausted={exhausted}")
    return phones, searched, cur, exhausted

def collect_numbers_batch(nicho: str, cities: List[str], limit: int, starts: Dict[str,int]|None=None) -> Tuple[List[str], int, Dict[str,int], bool]:
    """Percorre as cidades na ordem, respeitando offsets por cidade."""
    if starts is None: starts = {c:0 for c in cities}
    result: List[str] = []; searched_total = 0; flags = {c:False for c in cities}
    for c in cities:
        if len(result) >= limit: break
        if flags.get(c): continue
        remaining = limit - len(result)
        phones, searched, nxt, ex = collect_numbers_for_city(nicho, c, remaining, starts.get(c,0))
        searched_total += searched
        starts[c] = nxt; flags[c] = ex
        for t in phones:
            if t not in result:
                result.append(t)
                if len(result) >= limit: break
    exhausted_all = all(flags.get(c,False) for c in cities)
    return result, searched_total, starts, exhausted_all
