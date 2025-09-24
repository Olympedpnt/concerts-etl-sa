from __future__ import annotations
import re, uuid, logging, unicodedata, json
from datetime import datetime, timezone
from typing import List, Optional
from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright
from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

BASE_URL = "https://mio.dice.fm"
LIVE_URL = f"{BASE_URL}/events/live"
LOGIN_URL = f"{BASE_URL}/auth/login"

# --- utils ---

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

MONTHS = {
    "janv": 1, "jan": 1,
    "fevr": 2, "févr": 2, "fev": 2, "fe": 2,
    "mars": 3, "mar": 3,
    "avr": 4, "avril": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "aout": 8, "août": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12, "déc": 12
}

def _parse_fr_date(date_text: str, time_text: str) -> Optional[datetime]:
    if not date_text or not time_text:
        return None
    s = _strip_accents(date_text.lower()).replace(".", " ")
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", s)
    if not m:
        return None
    day, mon_key, year = int(m.group(1)), m.group(2)[:4], int(m.group(3))
    month = MONTHS.get(mon_key)
    if not month:
        return None
    tm = re.match(r"(\d{1,2}):(\d{2})", time_text.strip())
    if not tm:
        return None
    hh, mm = int(tm.group(1)), int(tm.group(2))
    try:
        return datetime(year, month, day, hh, mm)
    except Exception:
        return None

def _extract_id(href: str) -> str:
    m = re.search(r"/events/([^/]+)/", href or "")
    return m.group(1) if m else href or ""

# --- main ---

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    now = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome Safari"
        )
        page = await context.new_page()

        # --- login ---
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # email
        try:
            email = page.get_by_label(re.compile("Email address", re.I)).first
            await email.wait_for(state="visible", timeout=5000)
        except Exception:
            email = page.locator('input[type="email"]').first

        # password
        try:
            pwd = page.get_by_label(re.compile("Password", re.I)).first
            await pwd.wait_for(state="visible", timeout=5000)
        except Exception:
            pwd = page.locator('input[type="password"]').first

        await email.fill(settings.dice_email)
        await pwd.fill(settings.dice_password)

        submit = page.locator('button[type="submit"]').first
        await submit.click()

        try:
            await page.wait_for_url("**/events/live*", timeout=45000)
        except Exception:
            await page.goto(LIVE_URL, wait_until="domcontentloaded")

        await page.wait_for_load_state("networkidle")

        # --- events list ---
        selectors = [
            "div[class*='EventListItemGrid__EventListCard']",
            "div[data-testid='event-list-item']"
        ]
        found = None
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=15000)
                found = sel
                break
            except Exception:
                continue

        if not found:
            try:
                await page.screenshot(path="events_error.png", full_page=True)
                with open("events_error.html", "w", encoding="utf-8") as f:
                    f.write(await page.content())
            except Exception:
                pass
            log.warning("Aucun événement Dice détecté → retour liste vide")
            await context.close(); await browser.close()
            return []

        cards = await page.query_selector_all(found)

        # diag: combien de cartes
        try:
            with open("dice_cards_count.txt", "w", encoding="utf-8") as f:
                f.write(str(len(cards)))
        except Exception:
            pass

        out: List[NormalizedEvent] = []

        for card in cards:
            a = await card.query_selector("a")
            if not a:
                continue
            name = (await a.inner_text()).strip()
            href = await a.get_attribute("href")
            eid = _extract_id(href or "")

            date_el = await card.query_selector("span.EventCardValue__ValuePrimary-sc-14o65za-1")
            time_el = await card.query_selector("span.EventCardValue__ValueSecondary-sc-14o65za-2")
            date_txt = (await date_el.inner_text()).strip() if date_el else None
            time_txt = (await time_el.inner_text()).strip() if time_el else None
            dt_local = _parse_fr_date(date_txt, time_txt)

            # --- sold robust ---
            tickets_sold = None

            try:
                sold_el = await card.query_selector(
                    "div.EventPartSales__SalesWrapper-sc-khilk2-0 span.EventCardValue__ValuePrimary-sc-14o65za-1"
                )
                if sold_el:
                    txt = (await sold_el.inner_text()).strip()
                    m = re.search(r"(\d+)\s*/\s*\d+", txt.replace("\xa0", ""))
                    if m:
                        tickets_sold = int(m.group(1))
            except Exception:
                pass

            if tickets_sold is None:
                try:
                    descendants = await card.query_selector_all("*")
                    for d in descendants:
                        t = (await d.inner_text()).strip()
                        if "€" in t:
                            continue
                        m = re.search(r"(\d+)\s*/\s*\d+", t.replace("\xa0",""))
                        if m:
                            tickets_sold = int(m.group(1))
                            break
                except Exception:
                    pass

            if tickets_sold is None:
                try:
                    donut = await card.query_selector("div.CircleProgress__CircleProgressControl-sc-ac4mpo-0 span")
                    if donut:
                        t = (await donut.inner_text()).strip()
                        if t.isdigit():
                            tickets_sold = int(t)
                except Exception:
                    pass

            # debug diag pour 3 premières cartes
            try:
                if 'dice_diag_count' not in globals():
                    global dice_diag_count
                    dice_diag_count = 0
                if dice_diag_count < 3:
                    with open("dice_diag.txt", "a", encoding="utf-8") as f:
                        f.write(f"[card] name={name!r} date={date_txt!r} time={time_txt!r} sold={tickets_sold!r}\n")
                    dice_diag_count += 1
            except Exception:
                pass

            out.append(NormalizedEvent(
                provider="dice",
                event_id_provider=eid,
                event_name=name,
                city=None,
                country=None,
                event_datetime_local=dt_local,
                timezone="Europe/Paris",
                status="on sale",
                tickets_sold_total=tickets_sold,
                gross_total=None,
                net_total=None,
                currency="EUR",
                sell_through_pct=None,
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
            ))

        # dump json preview
        try:
            preview = [
                {
                    "name": e.event_name,
                    "dt": e.event_datetime_local.isoformat() if e.event_datetime_local else None,
                    "sold": e.tickets_sold_total
                }
                for e in out[:10]
            ]
            with open("dice_events_preview.json", "w", encoding="utf-8") as f:
                json.dump(preview, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        await context.close(); await browser.close()
        return out
