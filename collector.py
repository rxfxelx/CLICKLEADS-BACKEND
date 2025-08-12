import re, time, urllib.parse
from typing import List, Set, Tuple
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# Números BR típicos
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

def collect_numbers_ex(nicho: str, local: str, limite: int = 50) -> Tuple[List[str], bool, int]:
    """
    Retorna (numeros, exhausted_all, searched_total)
      - multi-cidades por vírgula/; (ex: "BH, Contagem; Betim")
      - paginação segura (start até 380)
      - searched_total = quantos únicos encontramos nesta rodada
    """
    out: List[str] = []
    seen: Set[str] = set()

    cidades = [c.strip() for c in re.split(r"[;,]", local) if c.strip()]
    if not cidades:
        cidades = [local.strip()]

    MAX_START = 380        # 0..380 (20 em 20) ~ até ~400 resultados
    PAGE_STEP = 20
    searched_total = 0
    exhausted_all = True   # se alguma cidade ainda tiver páginas, vira False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = browser.new_context(
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            )
            # corta mídia pra estabilidade/velocidade
            ctx.route(
                "**/*",
                lambda r: r.abort() if r.request.resource_type in {"image", "font", "media"} else r.continue_()
            )
            ctx.set_default_timeout(9000)
            ctx.set_default_navigation_timeout(18000)

            page = ctx.new_page()

            for cidade in cidades:
                if len(out) >= limite:
                    break

                q = urllib.parse.quote(f"{nicho} {cidade}")
                start = 0
                cidade_esgotada = False

                while len(out) < limite and start <= MAX_START:
                    url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
                    try:
                        page.goto(url, wait_until="domcontentloaded", timeout=18000)
                    except (PWTimeoutError, PWError):
                        start += PAGE_STEP
                        continue

                    _accept_consent(page)

                    # há lista?
                    try:
                        page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=7000)
                    except PWTimeoutError:
                        cidade_esgotada = True
                        break

                    # 1) tel: direto
                    found_this_page = False
                    try:
                        for a in page.locator('a[href^="tel:"]').all():
                            raw = (a.get_attribute("href") or "")[4:]
                            tel = norm_br_e164(raw)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                searched_total += 1
                                found_this_page = True
                                if len(out) >= limite:
                                    break
                    except Exception:
                        pass
                    if len(out) >= limite:
                        break

                    # 2) regex nos cartões
                    cards = page.locator("div[role='article'], div.VkpGBb")
                    if cards.count() == 0:
                        cidade_esgotada = True
                        break

                    textos = []
                    for el in cards.all():
                        try:
                            t = el.inner_text(timeout=1500)
                            if t:
                                textos.append(t)
                        except Exception:
                            pass
                    blob = "\n".join(textos)

                    for m in PHONE_RE.findall(blob):
                        tel = norm_br_e164(m)
                        if tel and tel not in seen:
                            seen.add(tel); out.append(tel)
                            searched_total += 1
                            found_this_page = True
                            if len(out) >= limite:
                                break

                    if not found_this_page and start >= MAX_START:
                        cidade_esgotada = True
                        break

                    start += PAGE_STEP
                    time.sleep(0.6)

                if not cidade_esgotada:
                    exhausted_all = False  # ainda havia páginas; mas seguimos limite
                # se esgotou esta cidade, tenta próxima
            ctx.close()
            browser.close()
    except Exception:
        # devolve o que tiver
        return out[:limite], True, searched_total

    # se percorremos todas as cidades até o fim das páginas, exhausted_all = True
    if len(out) < limite:
        exhausted_all = True

    return out[:limite], exhausted_all, searched_total

# Wrapper legada (se alguém chamar)
def collect_numbers(nicho: str, local: str, limite: int = 50) -> List[str]:
    nums, _, _ = collect_numbers_ex(nicho, local, limite)
    return nums
