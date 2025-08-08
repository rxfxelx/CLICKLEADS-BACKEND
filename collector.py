import re, time, urllib.parse
from typing import List, Set
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}[-.\s]?\d{4}")

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
                args=["--no-sandbox", "--disable-dev-shm-usage"],  # docker-friendly
            )
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            )
            # Bloqueia imagens/fonts pra acelerar
            ctx.route(
                "**/*",
                lambda r: r.abort() if r.request.resource_type in {"image", "font", "media"} else r.continue_()
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

                _accept_consent(page)

                # Garante que a lista carregou
                try:
                    page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=9000)
                except PWTimeoutError:
                    # Fallback: tenta achar número direto no body
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

                # Cards da página (prioriza role=article)
                cards = page.get_by_role("article")
                if cards.count() == 0:
                    cards = page.locator("div.VkpGBb, div[role='article']")
                qtd = cards.count()

                for i in range(qtd):
                    if len(out) >= limite:
                        break

                    try:
                        it = cards.nth(i)
                        it.scroll_into_view_if_needed()
                        it.click(force=True)
                        # Espera o painel abrir e exibir algo relacionado a telefone
                        page.wait_for_selector(
                            "a[href^='tel:'], span:has-text('Telefone'), div:has-text('Telefone')",
                            timeout=9000,
                        )
                    except PWTimeoutError:
                        continue

                    tel = None
                    # 1) Tenta link tel:
                    try:
                        link = page.locator("a[href^='tel:']").first
                        if link.count():
                            raw = (link.get_attribute("href") or "")[4:]
                            tel = norm_br_e164(raw)
                    except PWError:
                        pass

                    # 2) Fallback: varre o texto do painel
                    if not tel:
                        try:
                            blob = page.inner_text("body")
                            for m in PHONE_RE.findall(blob or ""):
                                tel = norm_br_e164(m)
                                if tel:
                                    break
                        except Exception:
                            pass

                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)

                    # Fecha o painel e volta pra lista
                    try:
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(150)
                    except Exception:
                        pass

                if len(out) >= limite:
                    break
                start += 20  # paginação do Local Finder
                time.sleep(1.0)

            ctx.close()
            browser.close()
    except Exception:
        return out[:limite]

    return out[:limite]
