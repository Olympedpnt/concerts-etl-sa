from __future__ import annotations

import re
import unicodedata
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Set

from concerts_etl.core.models import NormalizedEvent

# ---------------- Normalisation texte ----------------

_ARTICLE_PREFIX = re.compile(r"^(le|la|les|l|the)\s+", re.I)

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _strip_accents(s)
    s = s.lower()
    s = s.replace("’", "'")
    s = s.replace("&", "and")
    s = _ARTICLE_PREFIX.sub("", s).strip()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# petites aliases utiles pour salles courantes
VENUE_ALIASES = {
    "fgo barbara": "fgo-barbara",
    "fgo-barbara": "fgo-barbara",
    "le trianon": "trianon",
    "trianon": "trianon",
    "la cigale": "cigale",
    "cigale": "cigale",
    "le ferrailleur": "ferrailleur",
    "ferrailleur": "ferrailleur",
    "la maroquinerie": "la maroquinerie",
}

def _venue_canon(s: Optional[str]) -> str:
    n = _norm(s)
    return VENUE_ALIASES.get(n, n)

# ---------------- Dates ----------------

def _same_day(a: Optional[datetime], b: Optional[datetime]) -> bool:
    if not a or not b:
        return False
    return a.date() == b.date()

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    v = row.get("event_datetime_local")
    dt_key = v.isoformat() if isinstance(v, datetime) else (str(v) if v else "")
    return dt_key, (row.get("event_name") or "").lower()

# ---------------- Déduction artiste SG depuis event_name ----------------

# Pattern très permissif : "ARTISTE @ SALLE", "ARTISTE - SALLE", "ARTISTE — SALLE"
_SPLIT_RX = re.compile(r"\s*(?:@|-|–|—)\s*")

def _artist_from_name(event_name: str) -> Optional[str]:
    if not event_name:
        return None
    parts = _SPLIT_RX.split(event_name, maxsplit=1)
    candidate = parts[0].strip() if parts else ""
    return candidate or None

def _best_artist_guess(sg_artist: Optional[str], sg_event_name: str, known_artists: Set[str]) -> Optional[str]:
    # 1) si SG a déjà l’artiste → garde
    if sg_artist and sg_artist.strip():
        return sg_artist.strip()
    # 2) essaie en amont via pattern "artist - venue"
    guess = _artist_from_name(sg_event_name or "")
    if guess:
        return guess
    # 3) sinon, tente un “contains” sur la liste d’artistes DICE connue
    norm_title = _norm(sg_event_name)
    best = None
    best_len = 0
    for a in known_artists:
        na = _norm(a)
        if na and na in norm_title and len(na) > best_len:
            best, best_len = a, len(na)
    return best

# ---------------- Consolidation ----------------

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:

    # Index DICE
    dice_by_avd: Dict[Tuple[str, str, date], NormalizedEvent] = {}
    dice_by_acd: Dict[Tuple[str, str, date], NormalizedEvent] = {}  # (artist, city, date)

    dice_artist_set: Set[str] = set()

    for dc in dice_events or []:
        a = _norm(dc.artist_name or dc.event_name)
        v = _venue_canon(dc.venue_name) or _venue_canon(dc.city)
        c = _norm(dc.city)
        if dc.event_datetime_local:
            d = dc.event_datetime_local.date()
            dice_by_avd[(a, v, d)] = dc
            dice_by_acd[(a, c, d)] = dc
        if dc.artist_name:
            dice_artist_set.add(dc.artist_name)

    rows: List[Dict[str, Any]] = []
    used_dice_ids: Set[str] = set()

    # 1) On part de SG, on tente un match DICE
    for sg in shotgun_events or []:
        # artiste SG (déduction si vide)
        artist = _best_artist_guess(sg.artist_name, sg.event_name, dice_artist_set) or sg.event_name
        a = _norm(artist)
        v = _venue_canon(sg.venue_name) or _venue_canon(sg.city)
        c = _norm(sg.city)

        match: Optional[NormalizedEvent] = None
        if sg.event_datetime_local:
            d = sg.event_datetime_local.date()
            # priorité : (artist, venue, day)
            match = dice_by_avd.get((a, v, d))
            # fallback : (artist, city, day)
            if not match:
                match = dice_by_acd.get((a, c, d))

        # construit la ligne
        row: Dict[str, Any] = {
            "event_name": sg.event_name,
            "event_datetime_local": sg.event_datetime_local,
            "shotgun_tickets_sold": sg.tickets_sold_total,
            "dice_tickets_sold": match.tickets_sold_total if match else None,
            "artist": artist,
            "venue": sg.venue_name or sg.city,
        }
        row["shotgun_event_id"] = sg.event_id_provider
        if match:
            row["dice_event_id"] = match.event_id_provider
            used_dice_ids.add(match.event_id_provider)
        rows.append(row)

    # 2) Ajoute les DICE restants (non appariés)
    for dc in dice_events or []:
        if dc.event_id_provider in used_dice_ids:
            continue
        rows.append({
            "event_name": dc.event_name,
            "event_datetime_local": dc.event_datetime_local,
            "shotgun_tickets_sold": None,
            "dice_tickets_sold": dc.tickets_sold_total,
            "artist": dc.artist_name or dc.event_name,
            "venue": dc.venue_name or dc.city,
            "dice_event_id": dc.event_id_provider,
        })

    rows.sort(key=_sort_key)
    return rows
