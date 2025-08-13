# collector.py
import re
import time
import urllib.parse
from typing import Dict, List, Set, Tuple

import phonenumbers
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeoutError,
)

# Regex genérica para capturar possíveis telefones e deixar a
# validação/normalização para o phonenumbers (Brasil)
PHONE_RE = re.compile(r"\+?(\d[\d .()\-]{8,}\d)")


def norm_br_e164(raw: str) -> str | None:
    """Normaliza qualquer telefone detectado para +E164 Brasil (+55...)."""
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
    """Fecha banners de consentimento do Google quando aparecerem."""
    sels = [
        "#L2AGLb",
        "button:has-text('Aceitar tudo')",
        "button:has-text('Concordo')",
        "button:has-text('I agree')",
        "div[role=button]:has-text('Aceitar tudo')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel)
            if btn.count():
                btn.first.click(timeout=2000)
                page.wait_for_timeout(200)
                break
        except Exception:
            pass


def _numbers_from_local_page(page) -> List[str]:
    """
    Extrai números da página local do Google:
      1) <a href="tel:...">
      2) Regex no texto do body (cartões + painel lateral)
    """
    out: List[str] = []
    seen: Set[str] = set()

    # 1) Ancoras tel:
    try:
        for a in page.locator('a[href^="tel:"]').all():
            raw = (a.get_attribute("href") or "").replace("tel:", "")
            tel = norm_br_e164(raw)
            if tel and tel not in seen:
                seen.add(tel)
                out.append(tel)
    except Exception:
        pass

    # 2) Regex no body
    try:
        blob = page.inner_text("body")
        for m in PHONE_RE.findall(blob or ""):
            tel = norm_br_e164(m)
            if tel and tel not in seen:
                seen.add(tel)
                out.append(tel)
    except Exception:
        pass

    return out


def collect_numbers_for_city(
    nicho: str,
    city: str,
    limit: int,
    start: int,
) -> Tuple[List[str], int, int, bool]:
    """
    Coleta até 'limit' números para UMA cidade, iniciando na paginação 'start'
    (0, 20, 40...). Retorna:
      (phones, searched, next_start, exhausted_city)

    - phones: números únicos coletados (E.164)
    - searched: quantidade de ocorrências lidas (aprox.)
    - next_start: offset para próxima iteração nessa cidade
    - exhausted_city: True se não há mais o que paginar/obter
    """
    phones: List[str] = []
    searched = 0
    exhausted_city = False

    q = urllib.parse.quote(f"{nicho} {city}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        ctx = browser.new_context(
            locale="pt-BR",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        # Bloqueia recursos pesados para acelerar e reduzir chance de bloqueio
        ctx.route(
            "**/*",
            lambda r: r.abort()
            if r.request.resource_type in {"image", "font", "media"}
            else r.continue_(),
        )
        ctx
        
