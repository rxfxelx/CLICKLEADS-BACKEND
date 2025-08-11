# collector.py — robusto p/ Google Local (tbm=lcl) + várias cidades
import re, time, urllib.parse
from typing import List, Set, Tuple, Dict
import phonenumbers
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Error as PWError

# telefone BR (bem permissiva) + normalização E.164
PHONE_RE = re.compile(r"\+?\d[\d .()\-]{8,}\d")

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
    sels = [
        "#L2AGLb", "#introAgreeButton",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button[aria-label='Aceitar tudo']",
        "button:has-text('Accept all')",
        "button[aria-label='Accept all']",
        "div[role=button]:has-text('Aceitar')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=2000)
                page.wait_for_timeout(250)
                break
        except Exception:
            pass

def init_state(local: str) -> Dict:
    cities = [c.strip() for c in (local or "").split(",") if c.strip()]
    if not cities: cities = [local.strip() or ""]
    return {"cities": cities, "ci": 0, "start": 0, "done": [False]*len(cities)}

def _build_url(nicho: str, city: str, start: int) -> str:
    q = urllib.parse.quote(f"{nicho} {city}".strip())
    return f"https://www.google.com/search?tbm=lcl&hl=pt-BR&gl=BR&q={q}&start={start}"

def _pull_from_text(text: str, seen: Set[str], out: List[str], want: int):
    for m in PHONE_RE.findall(text or ""):
        tel = norm_br_e164(m)
        if tel and tel not in seen:
            seen.add(tel); out.append(tel)
            if len(out) >= want: break

def collect_batch(nicho: str, state: Dict, want: int) -> Tuple[List[str], Dict, bool]:
    cities, ci, start, done = state["cities"], state["ci"], state["start"], state["done"]
    out: List[str] = []
    seen: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]  # contêiner: memória/SHM
        )
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9"}
        )
        # bloquear recursos pesados p/ acelerar (Playwright route)  :contentReference[oaicite:1]{index=1}
        def intercept(route, request):
            if request.resource_type in {"image", "media", "font"}:
                return route.abort()
            return route.continue_()
        ctx.route("**/*", intercept)

        page = ctx.new_page()
        city_exhausted = False

        # avança até achar algo nesta cidade ou concluir que acabou
        while not done[ci] and len(out) < want:
            url = _build_url(nicho, cities[ci], start)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
            except PWTimeoutError:
                break

            _accept_consent(page)

            # 1) tenta anchors tel:
            try:
                tel_links = page.locator('a[href^="tel:"]')
                for i in range(tel_links.count()):
                    href = tel_links.nth(i).get_attribute("href") or ""
                    raw = href.replace("tel:", "")
                    tel = norm_br_e164(raw)
                    if tel and tel not in seen:
                        seen.add(tel); out.append(tel)
                        if len(out) >= want: break
            except PWError:
                pass

            # 2) coleta texto dos cartões + fallback HTML inteiro
            if len(out) < want:
                try:
                    # cartões típicos do Local Finder
                    blobs = []
                    for sel in ("div[role='article']", "div.VkpGBb", "div[aria-level]"):
                        els = page.locator(sel)
                        for k in range(min(50, els.count())):
                            try:
                                t = els.nth(k).inner_text(timeout=500)
                                if t: blobs.append(t)
                            except Exception:
                                pass
                    if not blobs:
                        # fallback: HTML bruto
                        html = page.content()
                        _pull_from_text(html, seen, out, want)
                    else:
                        _pull_from_text("\n".join(blobs), seen, out, want)
                except Exception:
                    pass

            # heurística de paginação/encerramento
            found_this_page = len(out) > 0
            if not found_this_page:
                # nada nesta página -> tenta próxima página uma vez; senão, esgota cidade
                if start == state["start"]:
                    start += 20
                else:
                    city_exhausted = True
            else:
                # se ainda quer mais, próxima página
                if len(out) < want:
                    start += 20

            # se decidiu esgotar cidade
            if city_exhausted:
                done[ci] = True
                # próxima cidade
                next_ci = None
                for idx, flag in enumerate(done):
                    if not flag:
                        next_ci = idx; break
                if next_ci is None:
                    break
                ci = next_ci
                start = 0
                city_exhausted = False

            # pequeno respiro p/ evitar throttle
            page.wait_for_timeout(350)

        ctx.close(); browser.close()

    # atualiza state
    state["ci"], state["start"], state["done"] = ci, start, done
    exhausted_all = all(done)
    return out, state, exhausted_all
