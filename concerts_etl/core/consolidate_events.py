from __future__ import annotations

import math, re, unicodedata
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from concerts_etl.core.models import NormalizedEvent

# -------- utils de normalisation --------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _floor_to_hour(dt: Optional[datetime]) -> Optional[datetime]:
    if not dt:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)

def _same_slot(a: Optional[datetime], b: Optional[datetime], tolerance_min: int = 60) -> bool:
    if not a or not b:
        return False
    delta = abs(a - b)
    return delta <= timedelta(minutes=tolerance_min)

def _event_key_triplet(e: NormalizedEvent) -> Tuple[str, str, Optional[datetime]]:
    # clé join = (artist_norm, venue_norm, heure-arrondie)
    return (
        _norm(e.artist_name or e.event_name),   # fallback sur le nom si pas d’artiste
        _norm(e.venue_name or e.city),
        _floor_to_hour(e.event_datetime_local),
    )

# -------- consolidation --------

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    # index Dice par (artist, venue) → buckets par heure-arrondie
    buckets: Dict[Tuple[str, str], List[NormalizedEvent]] = {}
    for ev in dice_events or []:
        a, v, h = _event_key_triplet(ev)
        buckets.setdefault((a, v), []).append(ev)

    rows: List[Dict[str, Any]] = []
    matched_dc: set[str] = set()

    # 1) pour chaque SG, on cherche un DC “proche” (≤ 60 min) sur la même paire (artist, venue)
    for sg in shotgun_events or []:
        a, v, h = _event_key_triplet(sg)
        candidates = buckets.get((a, v), [])
        match: Optional[NormalizedEvent] = None
        for dc in candidates:
            if _same_slot(sg.event_datetime_local, dc.event_datetime_local, tolerance_min=60):
                match = dc
                break

        row: Dict[str, Any] = {
            "event_name": sg.event_name or (match.event_name if match else ""),
            "event_datetime_local": sg.event_datetime_local or (match.event_datetime_local if match else None),
            "artist": sg.artist_name or (match.artist_name if match else ""),
            "venue": sg.venue_name or (match.venue_name if match else (sg.city or "")),
            "shotgun_tickets_sold": sg.tickets_sold_total,
            "dice_tickets_sold": match.tickets_sold_total if match else None,
        }
        if sg.event_id_provider:
            row["shotgun_event_id"] = sg.event_id_provider
        if match and match.event_id_provider:
            row["dice_event_id"] = match.event_id_provider
            matched_dc.add(match.event_id_provider)
        rows.append(row)

    # 2) ajoute les DC non appariés (pas d’équivalent SG)
    for dc in dice_events or []:
        if dc.event_id_provider in matched_dc:
            continue
        rows.append({
            "event_name": dc.event_name,
            "event_datetime_local": dc.event_datetime_local,
            "artist": dc.artist_name,
            "venue": dc.venue_name or dc.city,
            "shotgun_tickets_sold": None,
            "dice_tickets_sold": dc.tickets_sold_total,
            "dice_event_id": dc.event_id_provider,
        })

    # 3) tri par datetime puis nom (gestion str/datetime)
    def _sort_key(r: Dict[str, Any]):
        dt = r.get("event_datetime_local")
        dt_key = dt.isoformat() if isinstance(dt, datetime) else str(dt or "")
        return (dt_key, (r.get("event_name") or "").lower())

    rows.sort(key=_sort_key)
    return rows
