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

        # --- login (robuste) ---
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # 0) cookies (best-effort)
        try:
            for txt in [r"Accepter", r"Tout accepter", r"J.?accepte", r"Accept all"]:
                btn = page.get_by_role("button", name=re.compile(txt, re.I))
                if await btn.count() and await btn.first.is_visible():
                    await btn.first.click()
                    break
        except Exception:
            pass

        # 1) certains écrans affichent un choix de méthode → cliquer "Sign in with email" / "Continue with email"
        try:
            for txt in [r"Sign in with email", r"Continue with email", r"Se connecter.*email", r"Continuer.*email"]:
                b = page.get_by_role("button", name=re.compile(txt, re.I))
                if await b.count():
                    await b.first.click()
                    await page.wait_for_load_state("networkidle")
                    break
        except Exception:
            pass

        # DEBUG: dump après tentative de cliquer "Continue with email"
        try:
            await page.screenshot(path="dice_after_continue.png", full_page=True)
            with open("dice_after_continue.html", "w", encoding="utf-8") as f:
                f.write(await page.content())
        except Exception:
            pass


        # 2) helper pour trouver un input par différents moyens + frames
        async def find_input(selector_list) -> Optional[any]:
            # cherche d'abord sur la page principale
            for sel in selector_list:
                try:
                    loc = sel if hasattr(sel, "locator") is False else None
                except Exception:
                    loc = None
                # si `sel` est une string CSS
                target = page.locator(sel).first if isinstance(sel, str) else sel
                try:
                    await target.wait_for(state="visible", timeout=3000)
                    return target
                except Exception:
                    continue
            # sinon, parcourir les iframes visibles
            for f in page.frames:
                if f == page.main_frame:
                    continue
                for sel in selector_list:
                    target = f.locator(sel).first if isinstance(sel, str) else sel
                    try:
                        await target.wait_for(state="visible", timeout=2000)
                        return target
                    except Exception:
                        continue
            return None

        # 3) construire les candidats pour email & password
        email_candidates = [
            'input[type="email"]',
            'input[autocomplete="username"]',
            'input[name="email"]',
            'input[placeholder="Email address"]',
            # textbox par nom accessible (role)
            # (Playwright construit ce locator à l'exécution)
        ]
        pwd_candidates = [
            'input[type="password"]',
            'input[autocomplete="current-password"]',
            'input[name="password"]',
            'input[placeholder="Password"]',
        ]

        # aussi tenter par rôle/label (selon ton HTML "Email address"/"Password" sont souvent des labels)
        try:
            label_email = page.get_by_label(re.compile(r"Email address", re.I)).first
            await label_email.wait_for(state="visible", timeout=1000)
            email = label_email
        except Exception:
            try:
                role_email = page.get_by_role("textbox", name=re.compile(r"Email address", re.I)).first
                await role_email.wait_for(state="visible", timeout=1000)
                email = role_email
            except Exception:
                email = await find_input(email_candidates)

        try:
            label_pwd = page.get_by_label(re.compile(r"Password", re.I)).first
            await label_pwd.wait_for(state="visible", timeout=1000)
            pwd = label_pwd
        except Exception:
            try:
                role_pwd = page.get_by_role("textbox", name=re.compile(r"Password", re.I)).first
                await role_pwd.wait_for(state="visible", timeout=1000)
                pwd = role_pwd
            except Exception:
                pwd = await find_input(pwd_candidates)

        # dernier fallback: champs texte génériques visibles
        if email is None:
            email = await find_input(['input[type="text"]', 'input'])
        if pwd is None:
            pwd = await find_input(['input[type="password"]', 'input'])

        if email is None or pwd is None:
            try:
                await page.screenshot(path="login_error.png", full_page=True)
                with open("login_error.html", "w", encoding="utf-8") as f:
                    f.write(await page.content())
            except Exception:
                pass
            raise RuntimeError("Champs email/password introuvables sur l'écran de login Dice")

        # 4) remplir & soumettre
        await email.click()
        # DEBUG : dump avant tentative de remplir email
        try:
            await page.screenshot(path="dice_before_email.png", full_page=True)
            with open("dice_before_email.html", "w", encoding="utf-8") as f:
                f.write(await page.content())
        except Exception:
            pass
        await email.fill(settings.dice_email)
        await pwd.click()
        await pwd.fill(settings.dice_password)

        # bouton submit
        submit = None
        for loc in [
            page.locator('button[type="submit"]').first,
            page.get_by_role("button", name=re.compile(r"^(sign in|se connecter|log in)$", re.I)).first,
        ]:
            try:
                await loc.wait_for(state="visible", timeout=3000)
                submit = loc
                break
            except Exception:
                continue

        if submit:
            # certains UIs activent le bouton après blur
            try:
                await email.press("Tab")
                await pwd.press("Tab")
            except Exception:
                pass
            try:
                await submit.wait_for(state="enabled", timeout=5000)
                await submit.click()
            except Exception:
                await pwd.press("Enter")
        else:
            await pwd.press("Enter")

        # 5) aller/attendre la page des events
        try:
            await page.wait_for_url("**/events/live*", timeout=45000)
        except Exception:
            await page.goto(LIVE_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")


        # --- events list (robuste) ---
        # assure-toi que nous sommes bien sur /events/live et que la liste est hydratée
        await page.goto(LIVE_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # Attendre qu'au moins un lien d'événement apparaisse (beaucoup plus stable que les classes CSS)
        await page.wait_for_selector("a[href^='/events/']", timeout=30000)

        # dump diagnostic de la page après hydratation
        try:
            await page.screenshot(path="dice_events_loaded.png", full_page=True)
            with open("dice_events_loaded.html", "w", encoding="utf-8") as f:
                f.write(await page.content())
        except Exception:
            pass

        # scroll pour charger les ~10–20 événements
        async def auto_scroll():
            last = 0
            for _ in range(8):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
                h = await page.evaluate("document.body.scrollHeight")
                if h == last:
                    break
                last = h
        await auto_scroll()

        # Récupère tous les liens d'événement visibles
        links = await page.query_selector_all("a[href^='/events/']")
        cards_seen = set()
        out: List[NormalizedEvent] = []

        for a in links:
            href = await a.get_attribute("href")
            if not href or "/events/" not in href:
                continue

            # remonter au conteneur carte (le plus proche div/li/row contenant)
            card_js = await a.evaluate_handle("""
                (el) => {
                  let n = el;
                  for (let i = 0; i < 8 && n; i++) {
                    if (n.matches && (n.matches("div[data-testid='event-list-item']") ||
                                      n.matches("div[class*='EventListItemGrid__EventListCard']") ||
                                      n.matches("li") || n.matches("div"))) return n;
                    n = n.parentElement;
                  }
                  return el.parentElement;
                }
            """)
            card_el = card_js.as_element()
            if not card_el:
                continue  # sécurité : pas d'ElementHandle

            # éviter de traiter la même carte plusieurs fois (hash de l'outerHTML)
            try:
                outer = await card_el.evaluate("e => e.outerHTML")
                key = hash(outer)
                if key in cards_seen:
                    continue
                cards_seen.add(key)
            except Exception:
                pass

            # Nom
            name = (await a.inner_text()).strip()

            # ID provider
            eid = _extract_id(href)

            # Date / heure
            date_el = await card_el.query_selector("span.EventCardValue__ValuePrimary-sc-14o65za-1")
            time_el = await card_el.query_selector("span.EventCardValue__ValueSecondary-sc-14o65za-2")
            date_txt = (await date_el.inner_text()).strip() if date_el else None
            time_txt = (await time_el.inner_text()).strip() if time_el else None
            dt_local = _parse_fr_date(date_txt, time_txt)

            # Tickets vendus : cherche un motif X/Y n'importe où dans la carte
            tickets_sold = None

            # 1) zone “Billets”
            try:
                sold_node = await card_el.query_selector("div[class*='EventPartSales'] span[class*='EventCardValue__ValuePrimary']")
                if sold_node:
                    txt = (await sold_node.inner_text()).strip()
                    m = re.search(r"(\d+)\s*/\s*\d+", txt.replace("\xa0",""))
                    if m:
                        tickets_sold = int(m.group(1))
            except Exception:
                pass

            # 2) fallback : scanner tout le texte de la carte
            if tickets_sold is None:
                try:
                    nodes = await card_el.query_selector_all("*")
                    for n in nodes:
                        s = (await n.inner_text()).strip()
                        if not s or "€" in s:
                            continue
                        m = re.search(r"(\d+)\s*/\s*\d+", s.replace("\xa0",""))
                        if m:
                            tickets_sold = int(m.group(1))
                            break
                except Exception:
                    pass

            # 3) fallback “donut”
            if tickets_sold is None:
                try:
                    donut = await card_el.query_selector("div[class*='CircleProgress'] span")
                    if donut:
                        t = (await donut.inner_text()).strip()
                        if t.isdigit():
                            tickets_sold = int(t)
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
