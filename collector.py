import re, time, urllib.parse
from typing import List, Set
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\+?\d[\d .()-]{8,}\d")

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
                b.first.click(timeout=3000)
                page.wait_for_timeout(300)
                break
        except Exception:
            pass

def collect_numbers(nicho: str, local: str, limite: int = 50) -> List[str]:
    out: List[str] = []; seen: Set[str] = set()
    q = urllib.parse.quote(f"{nicho} {local}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox","--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            )
            # bloqueia imagens/fonts pra ficar leve
            ctx.route("**/*", lambda r: r.abort() if any(
                r.request.url.endswith(ext) for ext in (".png",".jpg",".jpeg",".webp",".gif",".svg",".woff",".woff2",".ttf")
            ) else r.continue_())
            ctx.set_default_timeout(9000); ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()
            url = f"https://www.google.com/maps/search/{q}?hl=pt-BR&gl=BR"
            page.goto(url, wait_until="domcontentloaded", timeout=18000)
            _accept_consent(page)

            # espera a lista do Maps
            page.wait_for_selector("div[role='feed'] div[role='article']", timeout=12000)

            cards = page.locator("div[role='feed'] div[role='article']")
            idx = 0
            while idx < cards.count() and len(out) < limite:
                try:
                    cards.nth(idx).scroll_into_view_if_needed()
                    cards.nth(idx).click()
                    # no painel do lugar, o botão 'Ligar/Telefone' traz o número no aria-label
                    page.wait_for_selector("button[aria-label*='Ligar'], button[aria-label*='Telefone'], button[aria-label^='Call']", timeout=12000)
                except PWTimeoutError:
                    idx += 1
                    continue

                tel = None
                try:
                    btn = page.locator("button[aria-label*='Ligar'], button[aria-label*='Telefone'], button[aria-label^='Call']").first
                    if btn.count():
                        aria = btn.get_attribute("aria-label") or ""
                        m = re.search(PHONE_RE, aria)
                        if m: tel = norm_br_e164(m.group(0))
                except PWError:
                    tel = None

                # fallback: varre texto do painel
                if not tel:
                    try:
                        blob = page.inner_text("div[role='region']")  # painel lateral
                    except Exception:
                        blob = page.inner_text("body")
                    for m in PHONE_RE.findall(blob or ""):
                        tel = norm_br_e164(m)
                        if tel: break

                if tel and tel not in seen:
                    seen.add(tel); out.append(tel)

                # volta pra lista (ESC)
                try:
                    page.keyboard.press("Escape"); page.wait_for_timeout(150)
                except Exception:
                    pass

                idx += 1
                # carrega mais cards conforme desce a lista
                if idx >= cards.count() and len(out) < limite:
                    page.mouse.wheel(0, 1200)
                    page.wait_for_timeout(600)
                    cards = page.locator("div[role='feed'] div[role='article']")

            ctx.close(); browser.close()
    except Exception:
        return out[:limite]

    return out[:limite]
