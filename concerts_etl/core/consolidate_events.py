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
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _event_date_str(e: Optional[NormalizedEvent]) -> str:
    """
    Renvoie la date (jour) d'un évènement en 'YYYY-MM-DD' (heure ignorée).
    """
    if not e or not e.event_datetime_local:
        return ""
    try:
        return e.event_datetime_local.date().isoformat()
    except Exception:
        # au cas où c'est déjà une string ou similaire
        v = e.event_datetime_local
        if isinstance(v, str):
            # on tente d'extraire la partie date au début
            m = re.match(r"(\d{4}-\d{2}-\d{2})", v)
            return m.group(1) if m else v
        return ""


def _key_by_artist_and_date(e: NormalizedEvent) -> Tuple[str, str]:
    """
    Clé de matching = (artist_norm, YYYY-MM-DD).
    On suppose que adapters/shotgun et adapters/dice fournissent artist_name.
    """
    artist = _norm_artist(getattr(e, "artist_name", "") or "")
    date_str = _event_date_str(e)
    return artist, date_str


def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    # on trie par date (string YYYY-MM-DD), puis nom d’event
    dt = row.get("event_datetime_local") or ""
    nm = (row.get("event_name") or "").lower()
    return str(dt), nm


def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusionne par (artist_name normalisé, date jour).
    La colonne 'event_datetime_local' contient la date seule (YYYY-MM-DD).
    """
    sg_map: Dict[Tuple[str, str], NormalizedEvent] = {}
    dc_map: Dict[Tuple[str, str], NormalizedEvent] = {}

    for ev in shotgun_events or []:
        sg_map[_key_by_artist_and_date(ev)] = ev

    for ev in dice_events or []:
        dc_map[_key_by_artist_and_date(ev)] = ev

    all_keys = set(sg_map.keys()) | set(dc_map.keys())
    rows: List[Dict[str, Any]] = []

    for k in all_keys:
        sg = sg_map.get(k)
        dc = dc_map.get(k)

        # Champs communs (privilégier Shotgun quand dispo, sinon DICE)
        event_name = (sg.event_name if sg else (dc.event_name if dc else "")).strip()
        # On affiche la date (jour) uniquement
        date_str = _event_date_str(sg) or _event_date_str(dc)

        # Artiste affiché tel quel (pas normalisé, pour lisibilité)
        artist_disp = (sg.artist_name if sg and sg.artist_name else (dc.artist_name if dc else "")) or ""

        # On tente de fournir un "lieu" lisible si présent dans l’un des deux
        venue_disp = ""
        if sg and getattr(sg, "venue_name", None):
            venue_disp = sg.venue_name or ""
        elif dc and getattr(dc, "venue_name", None):
            venue_disp = dc.venue_name or ""
        else:
            # fallback: ville si pas de venue_name
            if sg and sg.city:
                venue_disp = sg.city
            elif dc and dc.city:
                venue_disp = dc.city

        row: Dict[str, Any] = {
            "event_name": event_name,
            "event_datetime_local": date_str,                 # <-- date seule
            "artist": artist_disp,
            "venue": venue_disp,
            "shotgun_tickets_sold": sg.tickets_sold_total if sg else None,
            "dice_tickets_sold": dc.tickets_sold_total if dc else None,
        }

        if sg:
            row["shotgun_event_id"] = sg.event_id_provider
        if dc:
            row["dice_event_id"] = dc.event_id_provider

        rows.append(row)

    rows.sort(key=_sort_key)
    return rows
