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


# ------------------ Utils ------------------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


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
    if not dt_text:
        return None
    return dateparser.parse(
        dt_text.strip(),
        languages=["fr"],
        settings={
            "TIMEZONE": "Europe/Paris",
            "RETURN_AS_TIMEZONE_AWARE": False,
            "PREFER_DATES_FROM": "future",
        },
    )


def _guess_artist_and_venue(event_name: str, artist_hint: Optional[str] = None, venue_hint: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    artist = (artist_hint or "").strip() or None
    venue = (venue_hint or "").strip() or None
    if not artist or not venue:
        m = re.match(r"\s*(.+?)\s*(?:@|-|–|—)\s*(.+)\s*$", event_name or "", flags=re.IGNORECASE)
        if m:
            artist = artist or m.group(1).strip()
            venue = venue or m.group(2).strip()
    return artist, venue


def _fallback_artist_and_venue(e: NormalizedEvent) -> NormalizedEvent:
    if not e.artist_name or not e.artist_name.strip():
        m = re.match(r"\s*(.+?)\s*(?:@|-|–|—)\s*(.+)\s*$", e.event_name or "", flags=re.IGNORECASE)
        if m:
            e.artist_name = m.group(1).strip()
            e.venue_name = e.venue_name or m.group(2).strip()
        else:
            e.artist_name = (e.event_name or "").strip()
    return e


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

        try:
            btn = page.get_by_role("button", name=re.compile(r"(Accepter|Tout accepter|J.?accepte|Accept)", re.I)).first
            if await btn.is_visible(timeout=2000):
                await btn.click()
        except Exception:
            pass

        try:
            trigger = page.get_by_role("button", name=re.compile(r"(e.?mail|email)", re.I)).first
            if await trigger.is_visible(timeout=2000):
                await trigger.click()
        except Exception:
            pass

        email_input = page.locator('input[type="email"]').first
        pwd_input = page.locator('input[type="password"]').first
        await email_input.fill(settings.shotgun_email)
        await pwd_input.fill(settings.shotgun_password)

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

        await page.goto(EVENTS_URL, wait_until="domcontentloaded")

        try:
            tab_publie = page.get_by_role("tab", name=re.compile(r"publié", re.I))
            if await tab_publie.is_visible(timeout=2000):
                await tab_publie.click()
        except Exception:
            pass

        try:
            await page.wait_for_selector(".ant-statistic-content", timeout=15000)
        except Exception:
            pass

        async def auto_scroll():
            prev = 0
            for _ in range(10):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
                height = await page.evaluate("document.body.scrollHeight")
                if height == prev:
                    break
                prev = height

        await auto_scroll()

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
            await context.close(); await browser.close()
            return []

        out: List[NormalizedEvent] = []
        for c in cards:
            # --- Nom ---
            name_el = await c.query_selector("span.truncate.text-sm.font-medium")
            if not name_el:
                name_el = await c.query_selector("span.font-medium, h3, a[title]")
            event_name = (await name_el.inner_text()).strip() if name_el else None
            if not event_name:
                continue

            artist_el = await c.query_selector("[data-testid='artist-name'], .artist-name, .text-artist")
            artist_hint = (await artist_el.inner_text()).strip() if artist_el else None
            venue_el = await c.query_selector("[data-testid='venue-name'], .venue-name, .text-venue")
            venue_hint = (await venue_el.inner_text()).strip() if venue_el else None
            city_el = await c.query_selector("[data-testid='city-name'], .text-city, [class*='city']")
            city = (await city_el.inner_text()).strip() if city_el else None

            artist_name, venue_name = _guess_artist_and_venue(event_name, artist_hint, venue_hint or city)

            # --- Date ---
            event_dt = None
            time_el = await c.query_selector("time[datetime]")
            if time_el:
                iso_val = await time_el.get_attribute("datetime")
                try:
                    event_dt = dateparser.parse(iso_val, settings={"RETURN_AS_TIMEZONE_AWARE": False})
                except Exception:
                    event_dt = None
            if event_dt is None:
                date_el = await c.query_selector("span.text-white-700.text-xs.font-normal")
                if not date_el:
                    date_el = await c.query_selector("time, [class*='text-xs'], [data-testid='event-date']")
                dt_text = (await date_el.inner_text()).strip() if date_el else None
                event_dt = _parse_fr_datetime(dt_text)

            # --- Stats ---
            gross_total, tickets_total, sell_through_pct = None, None, None
            try:
                values = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-value")
                suffixes = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-suffix")

                def has_today(i: int) -> bool:
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
                    sell_through_pct = float(_parse_int(await pct_el.inner_text()) or 0)
            except Exception:
                pass

            status = "sold out" if "COMPLET" in (await c.inner_text()).upper() else "on sale"
            dt_key = event_dt.isoformat() if event_dt else None
            event_id_provider = _stable_event_id(event_name, dt_key)

            out.append(NormalizedEvent(
                provider="shotgun",
                event_id_provider=event_id_provider,
                event_name=event_name,
                city=city,
                country=None,
                event_datetime_local=event_dt,
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

        await context.close(); await browser.close()

        out = [_fallback_artist_and_venue(e) for e in out]
        log.info("Shotgun: %d events", len(out))
        return out
