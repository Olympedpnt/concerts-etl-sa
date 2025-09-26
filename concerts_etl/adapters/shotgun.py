from __future__ import annotations
import re, uuid, logging, hashlib, unicodedata
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright

from concerts_etl.core.models import RawShotgunCard, NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

LOGIN_URL = "https://smartboard.shotgun.live/fr/login?destination=%2Fevents"
EVENTS_URL = "https://smartboard.shotgun.live/events"


# ------------------ Utils ------------------

def _parse_money(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    t = text.replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    t = t.replace(".", "").replace(",", ".")
    m = re.findall(r"-?\d+(?:\.\d+)?", t)
    return (float(m[0]), "EUR") if m else (None, "EUR")

def _parse_int(text: str) -> Optional[int]:
    m = re.findall(r"\d+", text or "")
    return int(m[0]) if m else None

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()

def _stable_event_id(name: str, dt_text: Optional[str]) -> str:
    base = _slug(name or "event")
    key = f"{base}|{dt_text or ''}"
    return f"{base}-{hashlib.sha1(key.encode()).hexdigest()[:8]}"


def _guess_artist_and_venue(event_name: str, venue_hint: str | None = None):
    artist = None
    venue = None

    # Patterns simples: "ARTISTE @ LIEU" ou "ARTISTE - LIEU"
    m = re.match(r"\s*(.+?)\s*(?:@|-|–|—)\s*(.+)\s*$", event_name or "", flags=re.IGNORECASE)
    if m:
        artist = m.group(1).strip()
        venue = m.group(2).strip()

    # Si on a un hint fiable pour le lieu, on le priorise
    if venue_hint and venue_hint.strip():
        venue = venue_hint.strip()

    # nettoyage soft
    if artist:
        artist = re.sub(r"\s+", " ", artist)
    if venue:
        venue = re.sub(r"\s+", " ", venue)

    return artist, venue


# ------------------ Scraper ------------------

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def _collect_cards() -> List[RawShotgunCard]:
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            locale="fr-FR", timezone_id="Europe/Paris",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = await context.new_page()

        # ---------- LOGIN ----------
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # bouton cookies
        try:
            btn = page.get_by_role("button", name=re.compile(r"(Accepter|Tout accepter|J.?accepte)", re.I))
            if await btn.is_visible(timeout=2000):
                await btn.click()
        except Exception:
            pass

        # bouton "se connecter avec e-mail"
        try:
            trigger = page.get_by_role("button", name=re.compile(r"(e.?mail)", re.I))
            if await trigger.is_visible(timeout=2000):
                await trigger.click()
        except Exception:
            pass

        # champs email
        email_input = page.locator('input[type="email"]').first
        await email_input.fill(settings.shotgun_email)

        # champs password
        pwd_input = page.locator('input[type="password"]').first
        await pwd_input.fill(settings.shotgun_password)

        # bouton submit
        submit = page.locator('button[type="submit"]').first
        try:
            await submit.wait_for(state="enabled", timeout=8000)
            await submit.click()
        except Exception:
            await pwd_input.press("Enter")

        try:
            await page.wait_for_url(re.compile(r".*/events.*"), timeout=45000)
        except Exception:
            await page.goto(EVENTS_URL, wait_until="domcontentloaded")

        # ---------- EVENTS ----------
        await page.goto(EVENTS_URL, wait_until="domcontentloaded")

        # (1) Clique l'onglet "Publié" si présent
        try:
            tab_publie = page.get_by_role("tab", name=re.compile(r"publié", re.I))
            if await tab_publie.is_visible(timeout=2000):
                await tab_publie.click()
        except Exception:
            pass

        # (2) Scroll pour charger (infini)
        async def auto_scroll():
            prev = 0
            for _ in range(8):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
                height = await page.evaluate("document.body.scrollHeight")
                if height == prev:
                    break
                prev = height

        await auto_scroll()

        # (3) Sélecteurs de cartes
        selectors = [
            "div.relative.flex.h-full.w-full.flex-col",
            "[class*='relative'][class*='flex'][class*='flex-col']",
            "[data-testid='event-card']"
        ]
        cards = []
        for sel in selectors:
            cards = await page.query_selector_all(sel)
            if cards:
                break

        if not cards:
            try:
                await page.screenshot(path="events_empty.png", full_page=True)
                html = await page.content()
                with open("events.html", "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass
            return []

        results: List[RawShotgunCard] = []
        for c in cards:
            name_el = await c.query_selector("span.truncate.text-sm.font-medium")
            if not name_el:
                name_el = await c.query_selector("span.font-medium, h3, a[title]")
            name = (await name_el.inner_text()).strip() if name_el else None
            if not name:
                continue

            # date
            date_el = await c.query_selector("span.text-white-700.text-xs.font-normal")
            if not date_el:
                date_el = await c.query_selector("time, [class*='text-xs']")
            dt_text = (await date_el.inner_text()).strip() if date_el else None

            # stats
            values = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-value")
            suffixes = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-suffix")

            async def has_today(i: int) -> bool:
                if i < len(suffixes):
                    suf = (await suffixes[i].inner_text()).lower()
                    return "aujourd" in suf
                return False

            euros, ints = [], []
            for i, v in enumerate(values):
                txt = (await v.inner_text()).strip()
                if "€" in txt:
                    val, _ = _parse_money(txt)
                    euros.append((val, await has_today(i)))
                else:
                    ints.append((_parse_int(txt), await has_today(i)))

            gross_total = next((val for val, today in euros if not today), None)
            tickets_total = next((val for val, today in ints if not today), None)

            pct_el = await c.query_selector("span.text-xs.font-semibold, [class*='font-semibold']")
            sell_through_pct = float(_parse_int(await pct_el.inner_text()) or 0) if pct_el else None

            full_text = await c.inner_text()
            status = "sold out" if "COMPLET" in full_text.upper() else "on sale"

            event_id_provider = _stable_event_id(name, dt_text)

            event_dt = None
            if dt_text:
                try:
                    from datetime import datetime as _dt
                    for fmt in ("%a %d %b %Y %H:%M", "%a %d %b. %Y %H:%M"):
                        try:
                            event_dt = _dt.strptime(dt_text, fmt)
                            break
                        except Exception:
                            pass
                except Exception:
                    event_dt = None

            results.append(RawShotgunCard(
                event_id_provider=event_id_provider,
                event_name=name,
                event_datetime_local=event_dt,
                city=None,
                country=None,
                gross_total=gross_total,
                tickets_sold_total=tickets_total,
                sell_through_pct=sell_through_pct,
                currency="EUR",
                status=status,
                source_url=EVENTS_URL,
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
            ))

        await context.close()
        await browser.close()
        return results


# ------------------ Normalisation ------------------

def normalize(cards: List[RawShotgunCard]) -> List[NormalizedEvent]:
    out: List[NormalizedEvent] = []
    for c in cards:
        e = NormalizedEvent(
            provider="shotgun",
            event_id_provider=c.event_id_provider,
            event_name=c.event_name,
            city=c.city,
            country=c.country,
            event_datetime_local=c.event_datetime_local,
            timezone="Europe/Paris",
            status=c.status,
            tickets_sold_total=c.tickets_sold_total,
            gross_total=c.gross_total,
            net_total=None,
            currency=c.currency,
            sell_through_pct=c.sell_through_pct,
            scrape_ts_utc=c.scrape_ts_utc,
            ingestion_run_id=c.ingestion_run_id,
        )

        # enrich artist / venue
        artist, venue = _guess_artist_and_venue(c.event_name, venue_hint=c.city)
        e.artist_name = artist
        e.venue_name = venue or c.city

        out.append(e)
    return out


async def run() -> List[NormalizedEvent]:
    cards = await _collect_cards()
    return normalize(cards)
