# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from concerts_etl.core.models import NormalizedEvent


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm_artist(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _artist_key(e: NormalizedEvent) -> str:
    artist = e.artist_name or ""
    if not artist and e.event_name:
        m = re.match(r"\s*(.+?)\s*(?:@|-|–|—)\s*.+$", e.event_name)
        artist = (m.group(1).strip() if m else e.event_name.strip())
    return _norm_artist(artist)


def _date_key(e: NormalizedEvent) -> str:
    if isinstance(e.event_datetime_local, datetime):
        try:
            return e.event_datetime_local.date().isoformat()
        except Exception:
            return ""
    return ""


def _row_sort_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    v = row.get("event_datetime_local")
    dt_key = v.date().isoformat() if isinstance(v, datetime) else (str(v) if v else "")
    artist = (row.get("artist") or "").lower()
    name = (row.get("event_name") or "").lower()
    return dt_key, artist, name


def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    sg_map: Dict[Tuple[str, str], NormalizedEvent] = {}
    dc_map: Dict[Tuple[str, str], NormalizedEvent] = {}

    for ev in shotgun_events or []:
        k = (_artist_key(ev), _date_key(ev))
        if any(k):
            sg_map[k] = ev

    for ev in dice_events or []:
        k = (_artist_key(ev), _date_key(ev))
        if any(k):
            dc_map[k] = ev

    all_keys = set(sg_map.keys()) | set(dc_map.keys())
    rows: List[Dict[str, Any]] = []

    for k in all_keys:
        sg = sg_map.get(k)
        dc = dc_map.get(k)

        # Accès sûrs
        sg_name = sg.event_name if sg else ""
        dc_name = dc.event_name if dc else ""
        event_name = (sg_name or dc_name or "").strip()

        event_dt = (sg.event_datetime_local if (sg and sg.event_datetime_local)
                    else (dc.event_datetime_local if dc else None))

        sg_artist = sg.artist_name if sg else None
        dc_artist = dc.artist_name if dc else None
        artist = (sg_artist or dc_artist or "").strip()

        sg_venue = sg.venue_name if sg else None
        dc_venue = dc.venue_name if dc else None
        venue = (sg_venue or dc_venue or "").strip()

        row: Dict[str, Any] = {
            "event_name": event_name,
            "event_datetime_local": event_dt,
            "artist": artist,
            "venue": venue,
            "shotgun_tickets_sold": (sg.tickets_sold_total if sg else None),
            "dice_tickets_sold": (dc.tickets_sold_total if dc else None),
        }

        if sg:
            row["shotgun_event_id"] = sg.event_id_provider
        if dc:
            row["dice_event_id"] = dc.event_id_provider

        rows.append(row)

    rows.sort(key=_row_sort_key)
    return rows
