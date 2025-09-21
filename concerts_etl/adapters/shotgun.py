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
        await submit.click()

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

        # (2) Attends qu'au moins une statistique apparaisse (classe Ant Design)
        try:
            await page.wait_for_selector(".ant-statistic-content", timeout=15000)
        except Exception:
            # on continue, mais on dump si 0 carte
            pass

        # (3) Scroll pour charger (infini)
        async def auto_scroll():
            prev = 0
            for _ in range(8):   # 8 rafales suffisent pour une liste courte/moyenne
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
                height = await page.evaluate("document.body.scrollHeight")
                if height == prev:
                    break
                prev = height

        await auto_scroll()

        # (4) Sélecteurs de cartes — on tente plusieurs patterns
        selectors = [
            "div.relative.flex.h-full.w-full.flex-col",   # vu dans ton extrait
            "[class*='relative'][class*='flex'][class*='flex-col']",
            "[data-testid='event-card']"
        ]
        cards = []
        for sel in selectors:
            cards = await page.query_selector_all(sel)
            if cards:
                break

        # DEBUG si rien trouvé
        if not cards:
            try:
                await page.screenshot(path="events_empty.png", full_page=True)
                html = await page.content()
                with open("events.html", "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass
            return []  # pas de carte → l'ETL retournera no_data (et on aura artefacts)

        results: List[RawShotgunCard] = []
        for c in cards:
            # nom
            name_el = await c.query_selector("span.truncate.text-sm.font-medium")
            if not name_el:
                # fallback: premier <span> gras dans le bloc
                name_el = await c.query_selector("span.font-medium, h3, a[title]")
            name = (await name_el.inner_text()).strip() if name_el else None
            if not name:
                continue

            # date locale
            date_el = await c.query_selector("span.text-white-700.text-xs.font-normal")
            if not date_el:
                date_el = await c.query_selector("time, [class*='text-xs']")
            dt_text = (await date_el.inner_text()).strip() if date_el else None

            # stats € et #
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

            gross_total = gross_today = None
            for val, today in euros:
                if today: gross_today = val
                elif gross_total is None: gross_total = val

            tickets_total = tickets_today = None
            for val, today in ints:
                if today: tickets_today = val
                elif tickets_total is None: tickets_total = val

            pct_el = await c.query_selector("span.text-xs.font-semibold, [class*='font-semibold']")
            sell_through_pct = float(_parse_int(await pct_el.inner_text()) or 0) if pct_el else None

            full_text = await c.inner_text()
            status = "sold out" if "COMPLET" in full_text.upper() else "on sale"

            event_id_provider = _stable_event_id(name, dt_text)

            event_dt = None
            if dt_text:
                try:
                    cleaned = (dt_text
                               .replace("lun.", "lun").replace("mar.", "mar").replace("mer.", "mer")
                               .replace("jeu.", "jeu").replace("ven.", "ven").replace("sam.", "sam").replace("dim.", "dim")
                               .replace("janv.", "janv").replace("févr.", "fév").replace("avr.", "avr")
                               .replace("juil.", "juil").replace("sept.", "sept").replace("oct.", "oct")
                               .replace("nov.", "nov").replace("déc.", "déc"))
                    # best-effort; si fail -> None
                    from datetime import datetime as _dt
                    for fmt in ("%a %d %b %Y %H:%M", "%a %d %b. %Y %H:%M"):
                        try:
                            event_dt = _dt.strptime(cleaned, fmt); break
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
                gross_today=gross_today,
                tickets_sold_total=tickets_total,
                sell_through_pct=sell_through_pct,
                currency="EUR",
                status=status,
                source_url=EVENTS_URL,
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
            ))

        await context.close(); await browser.close()
        return results


# ------------------ Normalisation ------------------

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
