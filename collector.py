def collect_numbers(nicho: str, local: str, limite: int = 50) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    q = urllib.parse.quote(f"{nicho} {local}")

    try:
        with sync_playwright() as p:
            # contexto persistente + bloqueio de recursos = mais rápido / menos fricção
            ctx = p.chromium.launch_persistent_context(
                user_data_dir="/tmp/pw-data",
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                locale="pt-BR",
                user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            )
            ctx.route("**/*", lambda r: r.abort()
                      if r.request.resource_type in {"image","font","stylesheet","media"}
                      else r.continue_())
            ctx.set_default_timeout(6000)
            ctx.set_default_navigation_timeout(12000)

            page = ctx.new_page()
            start = 0

            while len(out) < limite:
                url = f"https://www.google.com/search?tbm=lcl&q={q}&hl=pt-BR&gl=BR&start={start}"
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=12000)
                except PWTimeoutError:
                    break

                _accept_consent(page)

                # garante lista
                try:
                    page.wait_for_selector("div[role='article'], div.VkpGBb, div[role='feed']", timeout=7000)
                except PWTimeoutError:
                    break

                # ---------- PASSO RÁPIDO (sem clique) ----------
                feed = page.locator("div[role='feed']")
                cards = page.get_by_role("article")
                if cards.count() == 0:
                    cards = page.locator("div.VkpGBb, div[role='article']")

                # 1) tenta links tel: dentro de cada card
                for i in range(cards.count()):
                    if len(out) >= limite: break
                    try:
                        el = cards.nth(i)
                        # tenta <a href="tel:..."> dentro do card
                        tel_link = el.locator("a[href^='tel:']").first
                        if tel_link.count():
                            raw = (tel_link.get_attribute("href") or "")[4:]
                            tel = norm_br_e164(raw)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                continue
                        # fallback: texto do próprio card
                        txt = el.inner_text(timeout=1200)
                        for m in PHONE_RE.findall(txt or ""):
                            tel = norm_br_e164(m)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                break
                    except Exception:
                        pass

                # 2) varre o feed inteiro (rápido) se ainda faltam números
                if len(out) < limite and feed.count():
                    try:
                        txt = feed.inner_text(timeout=1500)
                        for m in PHONE_RE.findall(txt or ""):
                            tel = norm_br_e164(m)
                            if tel and tel not in seen:
                                seen.add(tel); out.append(tel)
                                if len(out) >= limite: break
                    except Exception:
                        pass

                # ---------- PASSO LENTO (fallback com clique) ----------
                if len(out) < limite:
                    qtd = cards.count()
                    for i in range(qtd):
                        if len(out) >= limite: break
                        try:
                            it = cards.nth(i)
                            it.scroll_into_view_if_needed()
                            it.click(force=True)
                            page.wait_for_selector(
                                "a[href^='tel:'], span:has-text('Telefone'), div:has-text('Telefone')",
                                timeout=6000,
                            )
                        except PWTimeoutError:
                            continue

                        tel = None
                        try:
                            link = page.locator("a[href^='tel:']").first
                            if link.count():
                                raw = (link.get_attribute("href") or "")[4:]
                                tel = norm_br_e164(raw)
                        except PWError:
                            pass

                        if not tel:
                            try:
                                panel = page.locator("div[role='dialog'], div[role='region'], div[aria-modal='true']")
                                blob = panel.inner_text() if panel.count() else ""
                                for m in PHONE_RE.findall(blob or ""):
                                    tel = norm_br_e164(m); if tel: break
                            except Exception:
                                pass

                        if tel and tel not in seen:
                            seen.add(tel); out.append(tel)

                        try:
                            page.keyboard.press("Escape")
                            page.wait_for_timeout(220)
                        except Exception:
                            pass

                if len(out) >= limite: break
                start += 20
                page.wait_for_timeout(220)

            ctx.close()
    except Exception:
        return out[:limite]

    return out[:limite]
