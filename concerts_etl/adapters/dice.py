from __future__ import annotations
import re, uuid, logging, unicodedata
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
    "fevr": 2, "févr": 2, "fev": 2,
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
    # ex: "ven. 10 oct. 2025", "19:30"
    if not date_text or not time_text:
        return None
    s = _strip_accents(date_text.lower())
    s = s.replace(".", " ")
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", s)
    if not m:
        return None
    day = int(m.group(1))
    mon_key = m.group(2)[:4]
    year = int(m.group(3))
    month = MONTHS.get(mon_key)
    if not month:
        return None
    tm = re.match(r"(\d{1,2}):(\d{2})", time_text.strip())
    if not tm:
        return None
    hh, mm = int(tm.group(1)), int(tm.group(2))
    try:
        # naïf en local; l'affichage restera "Europe/Paris" côté NormalizedEvent
        return datetime(year, month, day, hh, mm)
    except Exception:
        return None

def _extract_id(href: str) -> str:
    # /events/RXZlbnQ6NDk2NDE3/overview -> RXZlbnQ6NDk2NDE3
    m = re.search(r"/events/([^/]+)/", href or "")
    return m.group(1) if m else href or ""

def _parse_sold(text: str) -> Optional[int]:
    # "9/9" -> 9
    if not text:
        return None
    m = re.search(r"(\d+)\s*/\s*\d+", text.replace("\xa0", ""))
    return int(m.group(1)) if m else None

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
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
        )
        page = await context.new_page()

        # --- login ---
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # cookies (si présents)
        try:
            btn = page.get_by_role("button", name=re.compile(r"(Accepter|Tout accepter|J.?accepte|Accept all)", re.I))
            if await btn.is_visible(timeout=2000):
                await btn.click()
        except Exception:
            pass

        # helpers robustes (label -> role -> placeholder -> fallback)
        async def fill_textbox(name_text: str, secret: str = "", is_password: bool = False):
            el = None
            # 1) par LABEL explicite
            try:
                el = page.get_by_label(re.compile(fr"^{re.escape(name_text)}$", re.I)).first
                await el.wait_for(state="visible", timeout=4000)
            except Exception:
                el = None
            # 2) par ROLE textbox avec nom accessible
            if el is None:
                try:
                    el = page.get_by_role("textbox", name=re.compile(fr"^{re.escape(name_text)}$", re.I)).first
                    await el.wait_for(state="visible", timeout=4000)
                except Exception:
                    el = None
            # 3) par placeholder (au cas où)
            if el is None:
                placeholder = "Password" if is_password else "Email address"
                try:
                    el = page.locator(f'input[placeholder="{placeholder}"]').first
                    await el.wait_for(state="visible", timeout=3000)
                except Exception:
                    el = None
            # 4) dernier recours: premier input de type correspondant
            if el is None:
                css = 'input[type="password"]' if is_password else 'input[type="text"], input[type="email"]'
                try:
                    el = page.locator(css).first
                    await el.wait_for(state="visible", timeout=3000)
                except Exception:
                    el = None

            if el is None:
                raise RuntimeError(f"Champ {name_text} introuvable")

            await el.click()
            await el.fill(secret)

        try:
            await fill_textbox("Email address", settings.dice_email, is_password=False)
            await fill_textbox("Password", settings.dice_password, is_password=True)

            submit = None
            for loc in [
                page.locator('button[type="submit"]').first,
                page.get_by_role("button", name=re.compile(r"^(sign in|se connecter)$", re.I)).first,
            ]:
                try:
                    await loc.wait_for(state="visible", timeout=3000)
                    submit = loc
                    break
                except Exception:
                    continue
            if submit is None:
                raise RuntimeError("Bouton de connexion introuvable")

            await submit.click()
        except Exception:
            try:
                await page.screenshot(path="login_error.png", full_page=True)
                with open("login_error.html", "w", encoding="utf-8") as f:
                    f.write(await page.content())
            except Exception:
                pass
            raise RuntimeError("Impossible de remplir/valider le formulaire de login Dice")

        # redirection vers la liste
        try:
            await page.wait_for_url("**/events/live*", timeout=45000)
        except Exception:
            await page.goto(LIVE_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # --- events list ---
        # on est censé être sur /events/live
        try:
            await page.wait_for_url("**/events/live*", timeout=20000)
        except Exception:
            await page.goto(LIVE_URL, wait_until="domcontentloaded")

        await page.wait_for_load_state("networkidle")

        # selectors Dice (React génère souvent des suffixes différents)
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
            # dump debug
            try:
                await page.screenshot(path="events_error.png", full_page=True)
                with open("events_error.html", "w", encoding="utf-8") as f:
                    f.write(await page.content())
            except Exception:
                pass
            log.warning("Aucun événement Dice détecté → retour liste vide")
            await context.close(); await browser.close()
            return []   # ne plante pas, retourne liste vide

        # scroll pour charger ~10 cartes
        async def auto_scroll():
            last = 0
            for _ in range(6):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(600)
                h = await page.evaluate("document.body.scrollHeight")
                if h == last: break
                last = h
        await auto_scroll()

        cards = await page.query_selector_all(found)
        out: List[NormalizedEvent] = []

        for card in cards:
            # name + id
            a = await card.query_selector("a.EventListItemGrid__EventName-sc-7aonoz-8")
            if not a:
                continue
            name = (await a.inner_text()).strip()
            href = await a.get_attribute("href")
            eid = _extract_id(href or "")

            # date + time
            date_el = await card.query_selector("span.EventCardValue__ValuePrimary-sc-14o65za-1")
            time_el = await card.query_selector("span.EventCardValue__ValueSecondary-sc-14o65za-2")
            date_txt = (await date_el.inner_text()).strip() if date_el else None
            time_txt = (await time_el.inner_text()).strip() if time_el else None
            dt_local = _parse_fr_date(date_txt, time_txt)

            # sold "X/Y" -> X
            sold_el = await card.query_selector("div.EventPartSales__SalesWrapper-sc-khilk2-0 span.EventCardValue__ValuePrimary-sc-14o65za-1")
            sold_txt = (await sold_el.inner_text()).strip() if sold_el else ""
            tickets_sold = _parse_sold(sold_txt)

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

        await context.close(); await browser.close()
        return out
