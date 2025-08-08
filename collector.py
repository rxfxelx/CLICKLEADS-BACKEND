import re, time, urllib.parse
from typing import List, Set
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

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

def _consent(page):
    for sel in ("#L2AGLb",
                "button:has-text('Aceitar tudo')",
                "button:has-text('Concordo')",
                "button:has-text('I agree')",
                "div[role=button]:has-text('Aceitar tudo')"):
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=3000)
                page.wait_for_timeout(300)
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
            # bloqueia assets pesados pra reduzir erros/tempo
            ctx.route("**/*", lambda r: r.abort() if any(r.request.url.endswith(ext)
                    for ext in (".png",".jpg",".jpeg",".webp",".gif",".svg",".woff",".woff2",".ttf")) else r.continue_())
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

                _consent(page)

                # lista de resultados do Local Finder
                try:
                    page.wait_for_selector("div[role='article'], div.VkpGBb", timeout=9000)
                except PWTimeoutError:
                    # último fallback: regex no body
                    try:
                        body = page.inner_text("body")
                        for m in PHONE_RE.findall(body or ""):
                            tel = norm_br_e164(m)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                if len(out) >= limite: break
                    except Exception:
                        pass
                    break

                cards = page.locator("div[role='article'], div.VkpGBb")
                qtd = cards.count()

                for i in range(qtd):
                    if len(out) >= limite: break
                    # abre o painel do card
                    try:
                        cards.nth(i).click()
                        page.wait_for_selector("a[href^='tel:'], div:has-text('Telefone')", timeout=9000)
                    except PWTimeoutError:
                        continue

                    # 1) tenta link tel:
                    try:
                        tel_link = page.locator("a[href^='tel:']").first
                        if tel_link.count():
                            raw = (tel_link.get_attribute("href") or "")[4:]
                            tel = norm_br_e164(raw)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                        else:
                            # 2) varre texto do painel
                            blob = page.inner_text("body")
                            for m in PHONE_RE.findall(blob or ""):
                                tel = norm_br_e164(m)
                                if tel and tel not in seen:
                                    seen.add(tel); out.append(tel); break
                    except PWError:
                        pass

                    # fecha painel (volta pra lista)
                    try:
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(150)
                    except Exception:
                        pass

                    if len(out) >= limite: break

                if len(out) >= limite: break
                start += 20  # próxima página
                time.sleep(1.2)

            ctx.close(); browser.close()
    except Exception:
        # não propaga erro; retorna o que tiver
        return out[:limite]

    return out[:limite]
