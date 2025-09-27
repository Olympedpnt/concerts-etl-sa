from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

from concerts_etl.core.config import settings
from concerts_etl.core.models import NormalizedEvent

log = logging.getLogger(__name__)

GRAPHQL_URL = "https://partners-endpoint.dice.fm/graphql"

# On ne passe que $after et $from (Datetime), on ne filtre pas par $to côté Dice
_EVENTS_QUERY = """
query Events($after: String, $from: Datetime) {
  viewer {
    events(first: 100, after: $after, where: { startDatetime: { gte: $from } }) {
      totalCount
      pageInfo { endCursor hasNextPage }
      edges {
        node {
          id
          name
          startDatetime
          currency
          artists { name }
          venues { name city country timezoneName }
          tickets(first: 1) { totalCount }
        }
      }
    }
  }
}
"""

def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # gère le 'Z' de fin
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None

def _pick_first(lst: Optional[List[Dict[str, Any]]], key: str) -> Optional[str]:
    if not lst:
        return None
    v = (lst[0] or {}).get(key)
    return v.strip() if isinstance(v, str) else v

async def _gql(client: httpx.AsyncClient, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    r = await client.post(GRAPHQL_URL, json={"query": query, "variables": variables}, timeout=30.0)
    r.raise_for_status()
    payload = r.json()
    if payload.get("errors"):
        raise RuntimeError(f"DICE GraphQL errors: {payload['errors']}")
    return payload["data"]

async def fetch_events() -> List[Dict[str, Any]]:
    token = getattr(settings, "dice_api_token", None)
    if not token:
        raise RuntimeError("DICE_API_TOKEN manquant (settings.dice_api_token).")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    # “pivot” = maintenant → on prend tout le futur
    from_lower = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    out: List[Dict[str, Any]] = []
    after: Optional[str] = None
    page = 0

    async with httpx.AsyncClient(headers=headers, timeout=15) as client:
        while True:
            page += 1
            data = await _gql(client, _EVENTS_QUERY, {"after": after, "from": from_lower})
            evs = data["viewer"]["events"]
            edges = evs.get("edges", [])
            out.extend([e["node"] for e in edges])

            has_next = evs.get("pageInfo", {}).get("hasNextPage", False)
            after = evs.get("pageInfo", {}).get("endCursor")
            log.info("Dice API: page %s, cumul %s événements", page, len(out))
            if not has_next:
                break

    log.info("Dice API: %s événements récupérés", len(out))
    return out

def _build_normalized(ev: Dict[str, Any]) -> NormalizedEvent:
    name = (ev.get("name") or "").strip()
    dt_local = _parse_iso(ev.get("startDatetime"))
    venues = ev.get("venues") or []
    artists = ev.get("artists") or []
    tickets = ev.get("tickets") or {}

    venue_name = _pick_first(venues, "name")
    city = _pick_first(venues, "city")
    country = _pick_first(venues, "country")
    tz = _pick_first(venues, "timezoneName") or "Europe/Paris"
    artist_name = _pick_first(artists, "name")

    tickets_sold = tickets.get("totalCount")
    if isinstance(tickets_sold, str) and tickets_sold.isdigit():
        tickets_sold = int(tickets_sold)

    currency = ev.get("currency")
    if isinstance(currency, str):
        currency = currency.strip()

    return NormalizedEvent(
        provider="dice",
        event_id_provider=ev.get("id") or "",
        event_name=name,
        city=city,
        country=country,
        event_datetime_local=dt_local,  # aware (UTC) mais on ne compare que la date côté consolidator
        timezone=tz,
        status="on sale",
        tickets_sold_total=tickets_sold,
        gross_total=None,
        net_total=None,
        currency=currency,
        sell_through_pct=None,
        scrape_ts_utc=datetime.now(timezone.utc),
        ingestion_run_id="dice-api",
        # clés de matching
        artist_name=artist_name,
        venue_name=venue_name or city,
    )

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    events = await fetch_events()
    loop = asyncio.get_event_loop()
    built = await asyncio.gather(*[loop.run_in_executor(None, _build_normalized, e) for e in events])
    return built
