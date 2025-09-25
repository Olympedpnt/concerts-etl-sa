# concerts_etl/adapters/dice.py
from __future__ import annotations

import re
import uuid
import json
import hashlib
import logging
import unicodedata
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright, Response, Request

from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

BASE_URL = "https://mio.dice.fm"
LIVE_URL = f"{BASE_URL}/events/live"
LOGIN_URL = f"{BASE_URL}/auth/login"

# ---------------- utils (gardé même si non utilisé ici) ----------------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

# ---------------- capture helpers ----------------

DUMP_DIR = Path("dice_api_dump")
RESP_DIR = DUMP_DIR / "responses"
REQ_DIR  = DUMP_DIR / "requests"

def _safe_name(s: str) -> str:
    # nommage stable et safe basé sur sha1(url+ts)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _ensure_dirs():
    RESP_DIR.mkdir(parents=True, exist_ok=True)
    REQ_DIR.mkdir(parents=True, exist_ok=True)
    (DUMP_DIR / "pages").mkdir(parents=True, exist_ok=True)

def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")

def _write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def _looks_interesting(url: str, content_type: str) -> bool:
    url_l = url.lower()
    ct_l = (content_type or "").lower()
    return (
        "application/json" in ct_l
        or "/graphql" in url_l
        or "/api/" in url_l
        or "/events" in url_l  # souvent présent dans les URLs / params
    )

# ---------------- main (CAPTURE UNIQUEMENT) ----------------

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    """
    Version spéciale : ne parse rien.
    Objectif : capturer toutes les requêtes/réponses JSON/GraphQL pour analyser l'API.
    Les fichiers sont écrits dans dice_api_dump/.
    """
    _ensure_dirs()
    now = datetime.now(timezone.utc).isoformat()

    index: list[dict] = []
    req_bodies: dict[str, str] = {}  # key = req_id (sha1), value = filename

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            locale="fr-FR",
            timezone_id="Europe/Paris",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome Safari"
            ),
        )
        page = await context.new_page()

        # --- hooks réseau ---

        async def on_request(req: Request):
            try:
                ct = req.headers.get("content-type", "")
                if req.method == "POST" and _looks_interesting(req.url, ct):
                    body = req.post_data or ""
                    key = _safe_name(f"{req.url}|{body}")
                    fn = f"{key}.txt"
                    _write_text(REQ_DIR / fn, body)
                    req_bodies[key] = fn
            except Exception:
                pass

        async def on_response(resp: Response):
            try:
                url = resp.url
                ct = resp.headers.get("content-type", "")
                if not _looks_interesting(url, ct):
                    return

                status = resp.status
                # filename based on url + status + time
                raw_id = f"{url}|{status}|{datetime.now(timezone.utc).isoformat()}"
                key = _safe_name(raw_id)
                fn = f"{key}.json"

                # on essaie JSON, sinon texte brut
                body_saved_as_json = False
                try:
                    data = await resp.json()
                    _write_json(RESP_DIR / fn, data)
                    body_saved_as_json = True
                except Exception:
                    try:
                        t = await resp.text()
                        # si c'est du texte mais JSON-likel, on le sauve en texte
                        _write_text(RESP_DIR / fn, t[:500_000])  # limite 500KB
                    except Exception:
                        _write_text(RESP_DIR / fn, "<unreadable>")

                # index
                entry = {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "url": url,
                    "method": resp.request.method,
                    "status": status,
                    "content_type": ct,
                    "response_file": f"responses/{fn}",
                }

                # essaie de recoller un body de requête si on a la même url + post
                if resp.request.method == "POST":
                    try:
                        body = resp.request.post_data or ""
                        body_key = _safe_name(f"{url}|{body}")
                        if body_key in req_bodies:
                            entry["request_file"] = f"requests/{req_bodies[body_key]}"
                    except Exception:
                        pass

                index.append(entry)
            except Exception:
                pass

        page.on("request", on_request)
        page.on("response", on_response)

        # --- LOGIN minimal (page simple avec inputs visibles dans tes captures) ---
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # Dump de la page de login pour vérification
        _write_text(DUMP_DIR / "pages" / "login.html", await page.content())

        # Inputs
        email = page.locator('input[type="email"]').first
        password = page.locator('input[type="password"]').first
        await email.fill(settings.dice_email)
        await password.fill(settings.dice_password)

        # Submit
        submit = page.get_by_role("button", name=re.compile(r"sign in", re.I)).first
        if await submit.count():
            await submit.click()
        else:
            await password.press("Enter")

        # Attendre la navigation post-login puis aller sur la liste
        try:
            await page.wait_for_url("**/events/**", timeout=45_000)
        except Exception:
            pass

        await page.goto(LIVE_URL, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle")

        # Dump de la page (même si contenu React vide)
        _write_text(DUMP_DIR / "pages" / "live_initial.html", await page.content())

        # Laisser respirer/lancer les XHR de la liste (et quelques scrolls doux)
        try:
            # petit scroll incrémental pour déclencher les fetchs
            for _ in range(8):
                await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
                await page.wait_for_timeout(700)
        except Exception:
            pass

        await page.wait_for_timeout(2_000)
        _write_text(DUMP_DIR / "pages" / "live_after_scroll.html", await page.content())

        # Sauvegarder l'état de session (cookies + localStorage)
        try:
            storage = await context.storage_state()
            _write_json(DUMP_DIR / "storage.json", storage)
        except Exception:
            pass

        # Sauvegarder un index récapitulatif
        try:
            meta = {
                "generated_at_utc": now,
                "count_entries": len(index),
                "base_url": BASE_URL,
                "live_url": LIVE_URL,
                "login_url": LOGIN_URL,
            }
            _write_json(DUMP_DIR / "index.json", {"meta": meta, "items": index})
        except Exception:
            pass

        await context.close()
        await browser.close()

    # Cette version ne renvoie volontairement aucun event (capture only)
    # On reviendra la prochaine étape avec un parseur fondé sur les JSON capturés.
    return []
