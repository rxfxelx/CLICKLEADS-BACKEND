import re, time, urllib.parse
from typing import List, Set
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

def norm_br_e164(raw: str):
    digits = re.sub(r"\D", "", raw or "")
    if not digits:
        return None
    if not digits.startswith("55"):
        digits = "55" + digits.lstrip("0")
    try:
        num = phonenumbers.parse("+" + digits, None)
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None

def _accept_consent_if_any(page):
    try:
        # botões comuns do consentimento do Google (pt/en)
        for sel in [
            "#L2AGLb",
            "button:has-text('Aceitar tudo')",
            "button:has-text('Concordo')",
            "button:has-text('I agree')",
            "div[role=button]:has-text('Aceitar tudo')",
        ]:
            btn = page.locator(sel)
            if btn.count() > 0:
                btn.first.click(timeout=3000)
                page.wait_for_timeout(400)
                break
    except Exception:
        pass

def collect_numbers(nicho: str, local: str, limite: int = 50) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    q = urllib.parse.quote(f"{nicho} {local}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
            )
            # bloqueia assets pesados
            ctx.route("**/*", lambda route: route.abort()
                      if any(ext in route.request.url for ext in (".png",".jpg",".jpeg",".webp",".gif",".svg",".woff",".woff2",".ttf"))
                      else route.continue_())
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()

            # opcional: stealth (se instalado)
            try:
                from playwright_stealth import stealth_sync  # pip install playwright-stealth
                stealth_sync(page)
            except Exception:
                pass

            start = 0
            while len(out) < limite:
                url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=18000)
                except PWTimeoutError:
                    break

                _accept_consent_if_any(page)

                # espera algo útil
                try:
                    page.wait_for_selector("a[href^='tel:'], div.VkpGBb, div[role=article]", timeout=9000)
                except PWTimeoutError:
                    # fallback: tenta pelo body completo; se nada, sai
                    try:
                        blob = page.inner_text("body", timeout=2000)
                        for m in PHONE_RE.findall(blob or ""):
                            tel = norm_br_e164(m)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                if len(out) >= limite: break
                    except Exception:
                        pass
                    if len(out) < limite:
                        break

                # 1) tel: links
                for a in page.locator('a[href^="tel:"]').all():
                    raw = (a.get_attribute("href") or "").replace("tel:", "")
                    tel = norm_br_e164(raw)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)
                        if len(out) >= limite: break
                if len(out) >= limite: break

                # 2) regex nos cartões
                txts = []
                for sel in ["div.VkpGBb", "div[role=article]", "div[aria-level]"]:
                    for el in page.locator(sel).all():
                        try:
                            t = el.inner_text()
                            if t: txts.append(t)
                        except PWError:
                            pass
                blob = "\n".join(txts)
                for m in PHONE_RE.findall(blob):
                    tel = norm_br_e164(m)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)
                        if len(out) >= limite: break

                start += 20
                time.sleep(1.5)

            ctx.close()
            browser.close()
    except (PWError, Exception):
        return out[:limite]

    return out[:limite]
