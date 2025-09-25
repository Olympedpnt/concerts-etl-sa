# concerts_etl/adapters/dice.py
from __future__ import annotations

import os
import uuid
import logging
from dataclasses import dataclass
from typing import List, Optional

from datetime import datetime, timezone

import asyncio
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

GRAPHQL_URL = "https://partners-endpoint.dice.fm/graphql"

# ---------------- helpers ----------------

def _get_token() -> str:
    # Priorité au settings si tu l'as ajouté, sinon variables d'env
    token = getattr(settings, "dice_api_token", None) or os.getenv("DICE_API_TOKEN")
    if not token:
        raise RuntimeError(
            "DICE_API_TOKEN manquant. Ajoute-le dans tes secrets/env (ne mets pas le token en dur)."
        )
    return token.strip()

def _parse_iso(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        # Les timestamps de l'API sont en ISO8601 (UTC ou tz-aware).
        d = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        # On renvoie en timezone locale inconnue -> garde l'offset reçu
        return d
    except Exception:
        return None

@dataclass
class DiceEvent:
    id: str
    name: Optional[str]
    start: Optional[datetime]
    capacity: Optional[int]
    sold: Optional[int]

# ---------------- GraphQL ----------------

EVENTS_QUERY = """
query Events($first:Int!, $after:String) {
  viewer {
    events(first: $first, after: $after) {
      pageInfo { endCursor hasNextPage }
      edges {
        node {
          id
          name
          startDatetime
          totalTicketAllocationQty
          tickets(first: 0) { totalCount }
        }
      }
      totalCount
    }
  }
}
"""

# Si tu veux calculer un "vendu net" (tickets - returns), on peut activer ce call.
RETURNS_COUNT_QUERY = """
query ReturnsCount($eventId: ID!) {
  viewer {
    returns(first: 0, where: { eventId: { eq: $eventId } }) {
      totalCount
    }
  }
}
"""

class DiceAPIError(Exception):
    pass

@retry(
    wait=wait_exponential(min=1, max=10),
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type((httpx.HTTPError, DiceAPIError)),
)
async def _gql(client: httpx.AsyncClient, query: str, variables: dict) -> dict:
    r = await client.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        # ne log pas le token ; les erreurs GraphQL n'en contiennent pas
        raise DiceAPIError(str(data["errors"]))
    return data["data"]

async def _fetch_events(client: httpx.AsyncClient, page_size: int = 50) -> List[DiceEvent]:
    events: List[DiceEvent] = []
    after: Optional[str] = None

    while True:
        payload = await _gql(client, EVENTS_QUERY, {"first": page_size, "after": after})
        conn = payload["viewer"]["events"]
        for edge in conn.get("edges", []):
            n = edge["node"]
            ev = DiceEvent(
                id=n["id"],
                name=n.get("name"),
                start=_parse_iso(n.get("startDatetime")),
                capacity=n.get("totalTicketAllocationQty"),
                sold=(n.get("tickets") or {}).get("totalCount"),
            )
            events.append(ev)

        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        after = page.get("endCursor")

    return events

async def _maybe_returns_count(client: httpx.AsyncClient, event_id: str) -> int:
    """Optionnel : compte des retours pour obtenir un net sold.
       Désactivé par défaut pour éviter une requête par event.
    """
    try:
        data = await _gql(client, RETURNS_COUNT_QUERY, {"eventId": event_id})
        return int((data["viewer"]["returns"] or {}).get("totalCount") or 0)
    except Exception:
        return 0

# ---------------- main ----------------

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
async def run() -> List[NormalizedEvent]:
    now = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    token = _get_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        # Un UA explicite pour la télémétrie côté DICE si besoin
        "User-Agent": "concerts-etl/1.0 (+partners-endpoint)",
    }

    out: List[NormalizedEvent] = []

    async with httpx.AsyncClient(headers=headers) as client:
        # 1) Récupère tous les events + sold totalCount
        events = await _fetch_events(client, page_size=50)
        log.info(f"Dice API: {len(events)} événements récupérés")

        # 2) Construis les NormalizedEvent
        #    Si tu veux le "net sold", active la section returns (attention N appels)
        count_returns = bool(os.getenv("DICE_COUNT_RETURNS", "").strip())

        async def build(ev: DiceEvent) -> NormalizedEvent:
            sold = ev.sold
            if count_returns and ev.id:
                ret = await _maybe_returns_count(client, ev.id)
                if sold is not None:
                    sold = max(0, sold - ret)

            return NormalizedEvent(
                provider="dice",
                event_id_provider=ev.id,
                event_name=ev.name or "",
                city=None,
                country=None,
                event_datetime_local=ev.start,
                timezone=None,  # l'API ne renvoie pas explicitement le tz de l'event
                status="on sale",  # pas exposé tel quel ici; ajuste si besoin plus tard
                tickets_sold_total=sold,
                gross_total=None,
                net_total=None,
                currency="EUR",  # facultatif : pas toujours pertinent ici
                sell_through_pct=None if ev.capacity in (None, 0) or sold is None
                                     else round(100.0 * sold / max(1, ev.capacity), 2),
                scrape_ts_utc=now,
                ingestion_run_id=run_id,
            )

        # parallélise modérément si returns activés
        if count_returns:
            sem = asyncio.Semaphore(8)
            async def _guarded_build(e: DiceEvent):
                async with sem:
                    return await build(e)
            built = await asyncio.gather(*[_guarded_build(e) for e in events])
        else:
            built = [await build(e) for e in events]

        out.extend(built)

    # Petit aperçu debug (non sensible)
    try:
        import json
        preview = [
            {
                "id": e.event_id_provider,
                "name": e.event_name,
                "dt": e.event_datetime_local.isoformat() if e.event_datetime_local else None,
                "sold": e.tickets_sold_total,
                "capacity": next((ev.capacity for ev in events if ev.id == e.event_id_provider), None),
            }
            for e in out[:20]
        ]
        with open("dice_api_preview.json", "w", encoding="utf-8") as f:
            json.dump(preview, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return out
