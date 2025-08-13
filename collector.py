import re
import time
import urllib.parse
from typing import List, Set, Tuple

import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Regex BR tolerante
PHONE_RE = re.compile(r"(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?\d{4,5}[-.\s]?\d{4}")

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

def _accept_consent(page) -> None:
    sels = [
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "div[role=button]:has-text('Aceitar tudo')",
        "button:has-text('I agree')",
    ]
    for sel in sels:
        try:
            el = page.locator(sel)
            if el.count():
                el.first.click(timeout=1500)
                page.wait_for_timeout(200)
                break
        except Exception:
            continue

def _is_block(page) -> bool:
    try:
        txt = ((page.title() or "") + " " + (page.inner_text("body") or "")).lower()
        if "unusual traffic" in txt or "captcha" in txt or "/sorry/" in (page.url or ""):
            return True
    except Exception:
        pass
    return False

def _scrape_page_numbers(page) -> Set[str]:
    found: Set[str] = set()
    # 1) tel:
    try:
        for el in page.locator("a[href^='tel:']").all():
            href = (el.get_attribute("href") or "")[4:]
            tel = norm_br_e164(href)
            if tel:
                found.add(tel)
    except Exception:
        pass
    # 2) regex body
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
    Coleta candidatos em E.164. Retorna (lista, exhausted_all).
    """
    target_pool = max(alvo * overscan_mult, alvo)
    out: List[str] = []
    seen: Set[str] = set()
    exhausted_all = True

    cities = [c.strip() for c in (local or "").split(",") if c.strip()]
    if not cities:
        cities = [local.strip()]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            )
            ctx.route("**/*", lambda r: r.abort()
                     if r.request.resource_type in {"image", "font", "media"}
                     else r.continue_())
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()

            for city in cities:
                start = 0
                empty_runs = 0
                while len(out) < target_pool:
                    q = urllib.parse.quote(f"{nicho} {city}")
                    url = f"https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=18000)
                    except PWTimeoutError:
                        empty_runs += 1
                        if empty_runs >= 2:
                            break
                        start += 20
                        continue

                    if _is_block(page):
                        exhausted_all = False
                        break

                    _accept_consent(page)
                    try:
                        page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=6000)
                    except Exception:
                        pass

                    before = len(seen)
                    nums = _scrape_page_numbers(page)
                    for tel in nums:
                        if tel not in seen:
                            seen.add(tel)
                            out.append(tel)
                            if len(out) >= target_pool:
                                break

                    added = len(seen) - before
                    if added == 0:
                        empty_runs += 1
                    else:
                        empty_runs = 0

                    if empty_runs >= 3:
                        break

                    start += 20
                    time.sleep(0.6)

                if len(out) >= target_pool:
                    break

            ctx.close()
            browser.close()
    except Exception:
        return out, False

    return out, exhausted_all
