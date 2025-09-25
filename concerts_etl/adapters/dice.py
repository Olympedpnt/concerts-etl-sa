# concerts_etl/adapters/dice.py
from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone as tzmod
from typing import List, Optional

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

API_URL = "https://partners-endpoint.dice.fm/graphql"


# ---------------- utils ----------------

def _parse_iso_dt_local(iso_str: str) -> Optional[datetime]:
    """
    Convertit un ISO8601 (ex: "2025-09-25T20:00:00Z") en datetime naïf,
    destiné à `event_datetime_local`. Le fuseau est renseigné à part.
    """
    if not iso_str:
        return None
    try:
        if iso_str.endswith("Z"):
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(iso_str)
        return dt.replace(tzinfo=None)  # naïf
    except Exception:
        return None


async def build(ev: dict) -> NormalizedEvent:
    eid = ev.get("id") or ev.get("eventIdLive") or ""
    name = ev.get("name") or ""

    # date/heure
    start_iso = ev.get("startDatetime")
    dt_local = _parse_iso_dt_local(start_iso)

    # fuseau horaire
    tz_name = None
    try:
        venues = ev.get("venues") or []
        if venues and isinstance(venues, list):
            tz_name = (venues[0] or {}).get("timezoneName")
    except Exception:
        pass
    timezone_str = tz_name or "Europe/Paris"

    # tickets vendus
    tickets_conn = ev.get("tickets") or {}
    tickets_sold = tickets_conn.get("totalCount")

    # allocation totale
    total_alloc = ev.get("totalTicketAllocationQty")

    # devise
    currency = ev.get("currency") or "EUR"

    return NormalizedEvent(
        provider="dice",
        event_id_provider=str(eid),
        event_name=name,
        city=None,
        country=None,
        event_datetime_local=dt_local,
        timezone=timezone_str,
        status="on sale",
        tickets_sold_total=tickets_sold,
        gross_total=None,
        net_total=None,
        currency=currency,
        sell_through_pct=None,  # à calculer si besoin via tickets_sold / total_alloc
        scrape_ts_utc=datetime.now(tzmod.utc),
        ingestion_run_id=str(uuid.uuid4()),
    )


# ---------------- requête API ----------------

QUERY_EVENTS = """
query events($first: Int!, $after: String) {
  viewer {
    events(first: $first, after: $after) {
      totalCount
      pageInfo {
        endCursor
        hasNextPage
      }
      edges {
        node {
          id
          name
          startDatetime
          totalTicketAllocationQty
          currency
          venues { timezoneName }
          tickets(first: 1) {
            totalCount
          }
        }
      }
    }
  }
}
"""


async def fetch_events() -> list[dict]:
    """
    Récupère tous les événements via l'API partenaire DICE.
    """
    token = settings.dice_api_token
    if not token:
        raise RuntimeError("DICE_API_TOKEN manquant (config/secrets)")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    events: list[dict] = []
    after = None
    client = httpx.AsyncClient(timeout=30)

    try:
        while True:
            variables = {"first": 50, "after": after}
            r = await client.post(
                API_URL, headers=headers, json={"query": QUERY_EVENTS, "variables": variables}
            )
            r.raise_for_status()
            data = r.json()
            viewer = (data.get("data") or {}).get("viewer") or {}
            evs = ((viewer.get("events") or {}).get("edges")) or []
            for e in evs:
                if e and e.get("node"):
                    events.append(e["node"])
            page_info = (viewer.get("events") or {}).get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
    finally:
        await client.aclose()

    log.info(f"Dice API: {len(events)} événements récupérés")
    return events


# ---------------- main entrypoint ----------------

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    events = await fetch_events()
    built = [await build(e) for e in events]
    return built
