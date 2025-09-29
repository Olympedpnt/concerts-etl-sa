# concerts_etl/adapters/shotgun.py
from __future__ import annotations

import re
import uuid
import hashlib
import logging
import unicodedata
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright

import dateparser

from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

LOGIN_URL = "https://smartboard.shotgun.live/fr/login?destination=%2Fevents"
EVENTS_URL = "https://smartboard.shotgun.live/events"


# ------------------ Utils parsing/texte ------------------

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )

def _parse_money(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    t = text.replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    t = t.replace(".", "").replace(",", ".")
    m = re.findall(r"-?\d+(?:\.\d+)?", t)
    return (float(m[0]), "EUR") if m else (None, "EUR")

def _parse_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.findall(r"\d+", text.replace("\u00a0", " ").replace("\u202f", " "))
    return int(m[0]) if m else None

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()

def _stable_event_id(name: str, dt_key: Optional[str]) -> str:
    base = _slug(name or "event")
    key = f"{base}|{dt_key or ''}"
    return f"{base}-{hashlib.sha1(key.encode()).hexdigest()[:8]}"

def _parse_fr_datetime(dt_text: Optional[str]) -> Optional[datetime]:
    """
    Parse FR, renvoie un datetime NAIF (local Europe/Paris) pour coller à timezone="Europe/Paris".
    Accepte aussi un ISO direct.
    """
    if not dt_text:
        return None

    # Direct ISO → essaye d'abord
    iso_try = dt_text.strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}T", iso_try):
            dt = dateparser.parse(iso_try, settings={"RETURN_AS_TIMEZONE_AWARE": False})
            if dt:
                return dt
    except Exception:
        pass

    # Phrases FR
    dt = dateparser.parse(
        dt_text,
        languages=["fr"],
        settings={
            "TIMEZONE": "Europe/Paris",
            "RETURN_AS_TIMEZONE_AWARE": False,
            "PREFER_DATES_FROM": "future",
        },
    )
    return dt

def _guess_artist_and_venue(event_name: str, artist_hint: Optional[str] = None, venue_hint: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Heuristique :
    - si artist_hint/venue_hint fournis → priorité
    - sinon essaie "ARTISTE @ LIEU" / "ARTISTE - LIEU"
    - sinon artiste = event_name (filet de secours)
    """
    artist = (artist_hint or "").strip() or None
    venue = (venue_hint or "").strip() or None

    if not artist or not venue:
        m = re.match(r"\s*(.+?)\s*(?:@|-|–|—)\s*(.+)\s*$", event_name or "", flags=re.IGNORECASE)
        if m:
            artist = artist or m.group(1).strip()
            venue = venue or m.group(2).strip()

    if not artist:
        artist = (event_name or "").strip() or None

    # nettoyage soft
    if artist:
        artist = re.sub(r"\s+", " ", artist)
    if venue:
        venue = re.sub(r"\s+", " ", venue)

    return artist, venue


# ------------------ Scraper principal ------------------

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = await context.new_page()

        # ---------- LOGIN ----------
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # cookies
        try:
            btn = page.get_by_role("button", name=re.compile(r"(Accepter|Tout accepter|J.?accepte|Accept)", re.I)).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
        except Exception:
            pass

        # "se connecter par e-mail"
        try:
            trigger = page.get_by_role("button", name=re.compile(r"(e.?mail|email)", re.I)).first
            if await trigger.is_visible(timeout=2000):
                await trigger.click()
        except Exception:
            pass

        # credentials
        email_input = page.locator('input[type="email"]').first
        pwd_input = page.locator('input[type="password"]').first
        await email_input.fill(settings.shotgun_email)
        await pwd_input.fill(settings.shotgun_password)

        submit = page.locator('button[type="submit"]').first
        try:
            await email_input.press("Tab")
            await pwd_input.press("Tab")
        except Exception:
            pass

        try:
            await submit.wait_for(state="enabled", timeout=8000)
            await submit.click()
        except Exception:
            await pwd_input.press("Enter")

        # ---------- EVENTS ----------
        try:
            await page.wait_for_url(re.compile(r".*/events.*"), timeout=45000)
        except Exception:
            await page.goto(EVENTS_URL, wait_until="domcontentloaded")

        await page.goto(EVENTS_URL, wait_until="domcontentloaded")

        # Onglet "Publié" si présent
        try:
            tab_publie = page.get_by_role("tab", name=re.compile(r"publié", re.I))
            if await tab_publie.is_visible(timeout=2000):
                await tab_publie.click()
        except Exception:
            pass

        # Attendre qu’un élément de stats OU une carte apparaisse (large filet)
        try:
            await page.wait_for_selector(
                ".ant-statistic-content, div.relative.flex.h-full.w-full.flex-col, [data-testid='event-card']",
                timeout=15000
            )
        except Exception:
            pass

        # Scroll pour charger (infini “soft”)
        async def auto_scroll():
            prev = 0
            for _ in range(12):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
                height = await page.evaluate("document.body.scrollHeight")
                if height == prev:
                    break
                prev = height

        await auto_scroll()

        # Récupération des cartes (plusieurs variantes)
        selectors = [
            "div.relative.flex.h-full.w-full.flex-col",                  # vu dans tes dumps
            "[class*='relative'][class*='flex'][class*='flex-col']",     # fallback large
            "[data-testid='event-card']",
            ".ant-card",                                                 # Ant Design card fallback
        ]
        cards = []
        used = set()
        for sel in selectors:
            try:
                found = await page.query_selector_all(sel)
                for c in found:
                    try:
                        outer = await c.evaluate("e => e.outerHTML.slice(0, 512)")
                        h = hash(outer)
                        if h not in used:
                            used.add(h)
                            cards.append(c)
                    except Exception:
                        cards.append(c)
            except Exception:
                continue

        # Fallback ultime : reconstruire par liens plausibles
        if not cards:
            try:
                links = await page.query_selector_all("a[href*='/events/']")
                for a in links:
                    # remonte vers un parent “carte”
                    card_handle = await a.evaluate_handle("""
                        (el) => {
                          let n = el;
                          for (let i=0; i<10 && n; i++) {
                            if (n.matches && (
                              n.matches("div.relative.flex.h-full.w-full.flex-col") ||
                              n.matches("[data-testid='event-card']") ||
                              n.matches(".ant-card") ||
                              n.matches("li") || n.matches("div")
                            )) return n;
                            n = n.parentElement;
                          }
                          return el.parentElement || el;
                        }
                    """)
                    ce = card_handle.as_element()
                    if ce:
                        cards.append(ce)
            except Exception:
                pass

        # Debug si rien
        if not cards:
            try:
                await page.screenshot(path="events_empty.png", full_page=True)
                html = await page.content()
                with open("events.html", "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass
            await context.close(); await browser.close()
            log.info("Shotgun: 0 événements parsés")
            return []

        out: List[NormalizedEvent] = []
        names_sample = []

        for c in cards:
            # --- Nom de l'événement
            name_el = await c.query_selector("span.truncate.text-sm.font-medium")
            if not name_el:
                name_el = await c.query_selector("span.font-medium, h3, a[title], [class*='font-medium']")
            event_name = (await name_el.inner_text()).strip() if name_el else None
            if not event_name:
                # petit filet : premier <a> “profond” avec un texte non vide
                a = await c.query_selector("a")
                if a:
                    txt = (await a.inner_text()).strip()
                    event_name = txt or None
            if not event_name:
                continue  # sans nom, on passe

            # --- Artiste/lieu hints si présents
            artist_el = await c.query_selector("[data-testid='artist-name'], .artist-name, .text-artist")
            artist_hint = (await artist_el.inner_text()).strip() if artist_el else None

            venue_el = await c.query_selector("[data-testid='venue-name'], .venue-name, .text-venue")
            venue_hint = (await venue_el.inner_text()).strip() if venue_el else None

            city_el = await c.query_selector("[data-testid='city-name'], .text-city, [class*='city']")
            city = (await city_el.inner_text()).strip() if city_el else None

            artist_name, venue_name = _guess_artist_and_venue(
                event_name,
                artist_hint=artist_hint,
                venue_hint=venue_hint or city,
            )

            # --- Date/heure locale (bétonnée) ---
            event_dt = None

            # 1) Balise <time datetime="...">
            t = await c.query_selector("time[datetime]")
            if t:
                try:
                    iso_val = await t.get_attribute("datetime")
                    if iso_val:
                        event_dt = dateparser.parse(
                            iso_val,
                            settings={"RETURN_AS_TIMEZONE_AWARE": False}
                        )
                except Exception:
                    event_dt = None

            # 2) Fallback: texte voisin (petit libellé date)
            if event_dt is None:
                date_el = await c.query_selector(
                    "span.text-white-700.text-xs.font-normal, "
                    "time, [data-testid='event-date'], [class*='text-xs']"
                )
                dt_text = (await date_el.inner_text()).strip() if date_el else None
                if dt_text:
                    event_dt = dateparser.parse(
                        dt_text,
                        languages=["fr"],
                        settings={
                            "TIMEZONE": "Europe/Paris",
                            "RETURN_AS_TIMEZONE_AWARE": False,
                            "PREFER_DATES_FROM": "future",
                        },
                    )

            # 3) Fallback ultime: on racle tout le texte de la carte et on cherche:
            #    - un ISO (2025-11-29T19:00)
            #    - ou un motif FR "ven. 10 oct. 2025 19:30" / "10 oct. 2025 19:30" / "10 octobre 2025 19:30"
            if event_dt is None:
                try:
                    raw = await c.inner_text()
                    # ISO
                    m = re.search(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?)", raw)
                    if m:
                        event_dt = dateparser.parse(
                            m.group(1),
                            settings={"RETURN_AS_TIMEZONE_AWARE": False}
                        )
                    if event_dt is None:
                        # FR courte (avec mois abrégé) ou longue
                        # ex: "ven. 10 oct. 2025 19:30" / "10 octobre 2025 19:30"
                        m = re.search(
                            r"(?:(?:lun|mar|mer|jeu|ven|sam|dim)\.?\s*)?"
                            r"(\d{1,2}\s+[A-Za-zéûîôàç\.]+\.?\s+\d{4}(?:\s+\d{1,2}:\d{2})?)",
                            raw, flags=re.IGNORECASE
                        )
                        if m:
                            event_dt = dateparser.parse(
                                m.group(1),
                                languages=["fr"],
                                settings={
                                    "TIMEZONE": "Europe/Paris",
                                    "RETURN_AS_TIMEZONE_AWARE": False,
                                    "PREFER_DATES_FROM": "future",
                                },
                            )
                except Exception:
                    pass

            # 4) Si on n'a toujours rien, trace courte pour debug
            if event_dt is None:
                try:
                    snippet = (await c.inner_text())[:200].replace("\n", " ")
                    log.debug("Shotgun: date introuvable pour %r ; snippet=%r", event_name, snippet)
                except Exception:
                    log.debug("Shotgun: date introuvable pour %r (no snippet)", event_name)


            # --- Statistiques (€, #, %)
            gross_total = None
            tickets_total = None
            sell_through_pct = None

            try:
                values = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-value")
                suffixes = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-suffix")

                def _suffix_is_today(idx: int, suf_nodes) -> bool:
                    try:
                        if idx < len(suf_nodes):
                            # Playwright Locator -> handle inner_text synchrone via eval
                            return False  # on ignore “aujourd'hui” pour l’instant
                    except Exception:
                        return False
                    return False

                euros, ints = [], []
                for i, v in enumerate(values):
                    txt = (await v.inner_text()).strip()
                    if "€" in txt:
                        val, _ = _parse_money(txt)
                        euros.append((val, _suffix_is_today(i, suffixes)))
                    else:
                        ints.append((_parse_int(txt), _suffix_is_today(i, suffixes)))

                for val, today in euros:
                    if not today:
                        gross_total = val
                        break
                for val, today in ints:
                    if not today:
                        tickets_total = val
                        break

                pct_el = await c.query_selector("span.text-xs.font-semibold, [class*='font-semibold']")
                if pct_el:
                    pct_txt = await pct_el.inner_text()
                    sell_through_pct = float(_parse_int(pct_txt) or 0)
            except Exception:
                pass

            # --- Statut
            full_text = (await c.inner_text()).upper()
            status = "sold out" if "COMPLET" in full_text else "on sale"

            # --- ID stable
            dt_key = event_dt.isoformat() if event_dt else None
            event_id_provider = _stable_event_id(event_name, dt_key)

            out.append(NormalizedEvent(
                provider="shotgun",
                event_id_provider=event_id_provider,
                event_name=event_name,
                city=city,
                country=None,
                event_datetime_local=event_dt,  # NAIF local
                timezone="Europe/Paris",
                status=status,
                tickets_sold_total=tickets_total,
                gross_total=gross_total,
                net_total=None,
                currency="EUR",
                sell_through_pct=sell_through_pct,
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
                artist_name=artist_name,
                venue_name=venue_name or city,
            ))

            if len(names_sample) < 10:
                names_sample.append(event_name)

        # Artefacts debug légers
        try:
            with open("shotgun_cards_count.txt", "w", encoding="utf-8") as f:
                f.write(f"cards_detected={len(cards)} parsed={len(out)} sample={names_sample}\n")
            await page.screenshot(path="shotgun_events.png", full_page=True)
            html = await page.content()
            with open("shotgun_events.html", "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

        await context.close(); await browser.close()
        log.info("Shotgun: %d événements parsés", len(out))
        return out
