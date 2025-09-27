# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set

from concerts_etl.core.models import NormalizedEvent


# ------------------- helpers texte / normalisation -------------------

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
    )

def _norm(s: Optional[str]) -> str:
    """Normalisation douce pour comparaisons (accents/casse/espaces)."""
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _event_date_str(e: Optional[NormalizedEvent]) -> str:
    """
    Renvoie la date (jour) 'YYYY-MM-DD' d'un évènement (heure ignorée).
    Accepte un datetime ou déjà une string ISO.
    """
    if not e or not e.event_datetime_local:
        return ""
    v = e.event_datetime_local
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, str):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", v)
        return m.group(1) if m else v
    return ""

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    # trie par date puis par nom d’event
    dt = row.get("event_datetime_local") or ""
    nm = (row.get("event_name") or "").lower()
    return str(dt), nm


# ------------------- consolidation -------------------

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusionne par DATE (jour uniquement) + 'artiste Shotgun' égal à
    ('artiste Dice' OU 'nom d’event Dice'), avec normalisation.

    Stratégie :
      1) Indexer les événements Shotgun par (date, artiste_norm).
      2) Parcourir les Dice. Pour chacun (date, artist_norm_dc) et (date, name_norm_dc),
         tenter un match dans l'index Shotgun. Si trouvé -> ligne fusionnée.
      3) Ajouter les Shotgun non appariés.
      4) Ajouter les Dice non appariés.

    La colonne `event_datetime_local` contient la **date seule** (YYYY-MM-DD).
    """
    # 1) index SG : (date, artiste_norm) -> NormalizedEvent
    sg_index: Dict[Tuple[str, str], NormalizedEvent] = {}
    for sg in shotgun_events or []:
        date_str = _event_date_str(sg)
        artist_norm = _norm(getattr(sg, "artist_name", "") or "")
        if date_str and artist_norm:
            sg_index[(date_str, artist_norm)] = sg

    used_sg: Set[str] = set()
    used_dc: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    # 2) tente d’apparier chaque Dice via (date, artist_norm) puis (date, name_norm)
    for dc in dice_events or []:
        date_dc = _event_date_str(dc)
        if not date_dc:
            continue

        dc_artist_norm = _norm(getattr(dc, "artist_name", "") or "")
        dc_name_norm = _norm(dc.event_name or "")

        sg_match: Optional[NormalizedEvent] = None

        # essai 1 : artiste Dice == artiste SG
        if dc_artist_norm:
            sg_match = sg_index.get((date_dc, dc_artist_norm))

        # essai 2 : nom d’event Dice == artiste SG
        if sg_match is None and dc_name_norm:
            sg_match = sg_index.get((date_dc, dc_name_norm))

        if sg_match and sg_match.event_id_provider not in used_sg:
            # ligne fusionnée
            artist_disp = (
                sg_match.artist_name
                or getattr(dc, "artist_name", "")
                or ""
            )
            venue_disp = (
                getattr(sg_match, "venue_name", None)
                or getattr(dc, "venue_name", None)
                or sg_match.city
                or dc.city
            )
            event_name = sg_match.event_name or dc.event_name or ""

            row: Dict[str, Any] = {
                "event_name": event_name,
                "event_datetime_local": date_dc,  # date jour uniquement
                "artist": artist_disp,
                "venue": venue_disp or "",
                "shotgun_tickets_sold": sg_match.tickets_sold_total,
                "dice_tickets_sold": dc.tickets_sold_total,
                "shotgun_event_id": sg_match.event_id_provider,
                "dice_event_id": dc.event_id_provider,
            }
            rows.append(row)
            used_sg.add(sg_match.event_id_provider)
            used_dc.add(dc.event_id_provider)

    # 3) ajoute SG non appariés
    for sg in shotgun_events or []:
        if sg.event_id_provider in used_sg:
            continue
        date_str = _event_date_str(sg)
        artist_disp = sg.artist_name or ""
        venue_disp = getattr(sg, "venue_name", None) or sg.city

        rows.append(
            {
                "event_name": sg.event_name or "",
                "event_datetime_local": date_str,
                "artist": artist_disp,
                "venue": venue_disp or "",
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": None,
                "shotgun_event_id": sg.event_id_provider,
            }
        )

    # 4) ajoute Dice non appariés
    for dc in dice_events or []:
        if dc.event_id_provider in used_dc:
            continue
        date_dc = _event_date_str(dc)
        venue_disp = getattr(dc, "venue_name", None) or dc.city

        rows.append(
            {
                "event_name": dc.event_name or "",
                "event_datetime_local": date_dc,
                "artist": getattr(dc, "artist_name", "") or "",
                "venue": venue_disp or "",
                "shotgun_tickets_sold": None,
                "dice_tickets_sold": dc.tickets_sold_total,
                "dice_event_id": dc.event_id_provider,
            }
        )

    rows.sort(key=_sort_key)
    return rows
