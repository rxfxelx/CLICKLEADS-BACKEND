import re, time, urllib.parse
from typing import List, Set
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

def norm_br_e164(raw: str):
    digits = re.sub(r"\D", "", raw or "")
    if not digits: return None
    if not digits.startswith("55"):
        digits = "55" + digits.lstrip("0")
    try:
        num = phonenumbers.parse("+" + digits, None)
        if phonenumbers.is_possible_number(num) and phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass
    return None

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
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)
            page = ctx.new_page()

            start = 0
            while len(out) < limite:
                url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=18000)
                except PWTimeoutError:
                    break

                # aceita consentimento se aparecer
                for sel in ["#L2AGLb",
                            "button:has-text('Aceitar tudo')",
                            "button:has-text('Concordo')",
                            "button:has-text('I agree')",
                            "div[role=button]:has-text('Aceitar tudo')"]:
                    try:
                        if page.locator(sel).count() > 0:
                            page.locator(sel).first.click(timeout=3000)
                            page.wait_for_timeout(300)
                            break
                    except Exception:
                        pass

                # cards da listagem (Local Finder)
                cards = page.locator("div[role='article']").filter(has_text=re.compile("."))  # bem genérico
                qtd = min(cards.count(), 20)
                if qtd == 0 and start == 0:
                    # fallback: tenta regex no body
                    try:
                        blob = page.inner_text("body")
                        for m in PHONE_RE.findall(blob or ""):
                            tel = norm_br_e164(m)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                if len(out) >= limite: break
                    except Exception:
                        pass

                for i in range(qtd):
                    if len(out) >= limite: break
                    # abre o painel de detalhes do i-ésimo resultado
                    try:
                        cards.nth(i).click()
                        page.wait_for_selector("a[href^='tel:'], div[aria-label*='Telefone'], div:has-text('Telefone')", timeout=9000)
                    except PWTimeoutError:
                        continue

                    # lê tel: no painel
                    try:
                        tel_link = page.locator("a[href^='tel:']").first
                        if tel_link.count():
                            raw = (tel_link.get_attribute("href") or "")[4:]
                            tel = norm_br_e164(raw)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                        else:
                            # fallback: caça número renderizado como texto
                            blob = page.inner_text("body")
                            for m in PHONE_RE.findall(blob or ""):
                                tel = norm_br_e164(m)
                                if tel and tel not in seen:
                                    seen.add(tel); out.append(tel)
                                    break
                    except PWError:
                        pass

                    # volta pra lista
                    try:
                        page.keyboard.press("Escape")  # fecha painel
                        page.wait_for_timeout(200)
                    except Exception:
                        pass

                if len(out) >= limite: break
                start += 20   # paginação de 20 em 20 no tbm=lcl
                time.sleep(1.2)

            ctx.close(); browser.close()
    except Exception:
        return out[:limite]

    return out[:limite]
