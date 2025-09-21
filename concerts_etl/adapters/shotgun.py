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

def _parse_money(text: str) -> Tuple[Optional[float], Optional[str]]:
    if not text:
        return None, None
    t = (text or "").strip()
    currency = "EUR" if "€" in t else None
    # enlever espaces insécables, etc.
    t = t.replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
    t = t.replace(".", "").replace(",", ".")
    m = re.findall(r"-?\d+(?:\.\d+)?", t)
    return (float(m[0]), currency) if m else (None, currency)

def _parse_int(text: str) -> Optional[int]:
    m = re.findall(r"\d+", text or "")
    return int(m[0]) if m else None

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return s

def _stable_event_id(name: str, dt_text: Optional[str]) -> str:
    base = _slug(name or "event")
    key = f"{base}|{dt_text or ''}"
    return f"{base}-{hashlib.sha1(key.encode()).hexdigest()[:8]}"

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def _collect_cards() -> List[RawShotgunCard]:
    """Se connecte et collecte les cartes d'événements de la liste Publié."""
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(locale="fr-FR", timezone_id="Europe/Paris")
        page = await context.new_page()

        # Login
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.fill('input[type="email"]', settings.shotgun_email)
        await page.fill('input[type="password"]', settings.shotgun_password)
        await page.click('button[type="submit"]')
        await page.wait_for_url("**/events*", timeout=45000)

        # Page événements
        await page.goto(EVENTS_URL)
        # La carte racine dans ton HTML commence par "div.relative.flex.h-full.w-full.flex-col ..."
        cards = await page.query_selector_all("div.relative.flex.h-full.w-full.flex-col")

        results: List[RawShotgunCard] = []
        for c in cards:
            name_el = await c.query_selector("span.truncate.text-sm.font-medium")
            name = (await name_el.inner_text()).strip() if name_el else None
            if not name:
                continue

            # Date FR
            date_el = await c.query_selector("span.text-white-700.text-xs.font-normal")
            dt_text = (await date_el.inner_text()).strip() if date_el else None

            # Stats: quatre blocs ant-statistic (€, € aujourd’hui, #, # aujourd’hui)
            value_spans = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-value")
            suffix_spans = await c.query_selector_all(".ant-statistic-content .ant-statistic-content-suffix")

            async def has_today(i: int) -> bool:
                if i < len(suffix_spans):
                    suf = (await suffix_spans[i].inner_text()).strip().lower()
                    return "aujourd" in suf
                return False

            euros: List[Tuple[Optional[float], bool]] = []
            ints: List[Tuple[Optional[int], bool]] = []

            for i, v in enumerate(value_spans):
                txt = (await v.inner_text()).strip()
                if "€" in txt:
                    val, _cur = _parse_money(txt)
                    euros.append((val, await has_today(i)))
                else:
                    ints.append((_parse_int(txt), await has_today(i)))

            gross_total = None
            gross_today = None
            for val, today in euros:
                if today:
                    gross_today = val
                else:
                    if gross_total is None:
                        gross_total = val

            tickets_sold_total = None
            tickets_sold_today = None
            for val, today in ints:
                if today:
                    tickets_sold_today = val
                else:
                    if tickets_sold_total is None:
                        tickets_sold_total = val

            # % sell-through
            pct_el = await c.query_selector("span.text-xs.font-semibold")
            sell_through_pct = None
            if pct_el:
                sell_through_pct = float(_parse_int(await pct_el.inner_text()) or 0)

            # Statut
            full_text = await c.inner_text()
            status = "sold out" if "COMPLET" in full_text.upper() else "on sale"

            # Ville indisponible dans l'extrait → None
            city = None

            # ID provider stable (nom+date)
            event_id_provider = _stable_event_id(name, dt_text)

            # Parse date FR (best effort)
            event_dt = None
            if dt_text:
                try:
                    # ex "ven. 10 oct. 2025 19:30"
                    # on enlève quelques points abréviations
                    cleaned = dt_text.replace("ven.", "ven").replace("oct.", "oct")
                    for fmt in ("%a %d %b %Y %H:%M",):
                        try:
                            # NB: pas de locale garantie en runner, on accepte échec → None
                            from datetime import datetime as _dt
                            event_dt = _dt.strptime(cleaned, fmt)
                            break
                        except Exception:
                            pass
                except Exception:
                    event_dt = None

            results.append(RawShotgunCard(
                event_id_provider=event_id_provider,
                event_name=name,
                event_datetime_local=event_dt,
                city=city,
                country=None,
                gross_total=gross_total,
                gross_today=gross_today,
                tickets_sold_total=tickets_sold_total,
                sell_through_pct=sell_through_pct,
                currency="EUR",
                status=status,
                source_url=EVENTS_URL,
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
            ))

        await context.close(); await browser.close()
        return results

def normalize(cards: List[RawShotgunCard]) -> List[NormalizedEvent]:
    return [
        NormalizedEvent(
            provider="shotgun",
            event_id_provider=c.event_id_provider,
            event_name=c.event_name,
            city=c.city,
            country=c.country,
            event_datetime_local=c.event_datetime_local,
            timezone="Europe/Paris",
            status=c.status,
            tickets_sold_total=c.tickets_sold_total,
            tickets_sold_today=c.tickets_sold_today,
            gross_total=c.gross_total,
            gross_today=c.gross_today,
            net_total=None,
            currency=c.currency,
            sell_through_pct=c.sell_through_pct,
            scrape_ts_utc=c.scrape_ts_utc,
            ingestion_run_id=c.ingestion_run_id,
        )
        for c in cards
    ]

async def run() -> List[NormalizedEvent]:
    cards = await _collect_cards()
    return normalize(cards)
