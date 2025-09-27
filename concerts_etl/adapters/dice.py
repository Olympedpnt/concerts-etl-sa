# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from concerts_etl.core.models import NormalizedEvent


# ------------------- helpers texte / normalisation -------------------

_STOPWORDS = {
    "the","and","feat","ft","with","x","&","+","-", "–","—",
    "le","la","les","l","de","du","des","et","au","aux","chez",
}

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm_basic(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _date_only(dt: Optional[datetime]) -> Optional[date]:
    return dt.date() if isinstance(dt, datetime) else None

def _artist_tokens(s: Optional[str]) -> List[str]:
    """
    Découpe un champ artiste/nom d’event en tokens utiles (accents enlevés).
    Séparateurs pris en compte: , ; / + & x - – — feat ft with @
    """
    if not s:
        return []
    s = _norm_basic(s)
    # remplace séparateurs multi-artistes par virgule
    s = re.sub(r"(,|/|&|\+|@| - | – | — |\bfeat\b|\bft\b|\bwith\b|\bx\b)", ",", s)
    # remplace aussi ' x ' (minuscule) explicitement
    s = re.sub(r"\s+x\s+", ",", s)
    # découpe par virgule puis par espace
    parts = []
    for chunk in s.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        # enlève doublons de ponctuation
        chunk = re.sub(r"[^\w\s]", " ", chunk)
        for tok in chunk.split():
            t = tok.strip()
            if len(t) <= 2:      # évite tokens trop courts (ex: 'le', 'la', etc.)
                continue
            if t in _STOPWORDS:
                continue
            parts.append(t)
    return parts

def _names_overlap(sg_artist: str, sg_event_name: str, dc_artist: str) -> bool:
    """
    True si l’artiste Dice est inclus dans l’artiste/nom Shotgun (ou via recouvrement de tokens).
    """
    a_sg = _norm_basic(sg_artist)
    n_sg = _norm_basic(sg_event_name)
    a_dc = _norm_basic(dc_artist)

    if not a_dc:
        return False

    # 1) inclusion brute (substring)
    if a_dc and (a_dc in a_sg or a_dc in n_sg):
        return True

    # 2) recouvrement de tokens (au moins 1 token en commun)
    toks_sg = set(_artist_tokens(sg_artist) + _artist_tokens(sg_event_name))
    toks_dc = set(_artist_tokens(dc_artist))
    return len(toks_sg & toks_dc) > 0


# ------------------- consolidation -------------------

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    v = row.get("event_date") or row.get("event_datetime_local")
    if isinstance(v, datetime):
        dt_key = v.isoformat()
    else:
        dt_key = str(v) if v else ""
    return dt_key, (row.get("event_name") or "").lower()

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusionne par DATE (jour uniquement) + recouvrement d'artiste.
    - On commence par indexer Dice par date → liste d'événements.
    - Pour chaque Shotgun du même jour, on cherche un Dice dont l’artiste recouvre.
    - Si match -> une seule ligne avec les colonnes shotgun_* et dice_*.
    - Sinon chaque event reste sur sa propre ligne.
    """
    # Index Dice par date
    dice_by_day: Dict[date, List[NormalizedEvent]] = {}
    for dc in dice_events or []:
        d = _date_only(dc.event_datetime_local)
        if not d:
            continue
        dice_by_day.setdefault(d, []).append(dc)

    used_dc: set[str] = set()
    rows: List[Dict[str, Any]] = []

    # 1) essaie de matcher chaque SG avec un DC du même jour
    for sg in shotgun_events or []:
        day = _date_only(sg.event_datetime_local)
        sg_artist = getattr(sg, "artist_name", "") or ""
        sg_name = sg.event_name or ""

        matched_dc: Optional[NormalizedEvent] = None
        if day in dice_by_day:
            for dc in dice_by_day[day]:
                if dc.event_id_provider in used_dc:
                    continue
                dc_artist = getattr(dc, "artist_name", "") or ""
                if _names_overlap(sg_artist, sg_name, dc_artist):
                    matched_dc = dc
                    used_dc.add(dc.event_id_provider)
                    break

        # construit la ligne fusionnée ou uniquement SG
        if matched_dc:
            event_name = sg.event_name or matched_dc.event_name or ""
            row: Dict[str, Any] = {
                "event_name": event_name,
                # on écrit la DATE (jour) comme chaîne "YYYY-MM-DD" pour l’export Sheet
                "event_date": day.isoformat() if day else "",
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": matched_dc.tickets_sold_total,
                "artist": sg_artist or getattr(matched_dc, "artist_name", ""),
                "venue": getattr(sg, "venue_name", None) or getattr(matched_dc, "venue_name", None) or sg.city or matched_dc.city,
                "shotgun_event_id": sg.event_id_provider,
                "dice_event_id": matched_dc.event_id_provider,
            }
            rows.append(row)
        else:
            # pas de match → ligne SG seule
            row: Dict[str, Any] = {
                "event_name": sg.event_name or "",
                "event_date": day.isoformat() if day else "",
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": None,
                "artist": sg_artist,
                "venue": getattr(sg, "venue_name", None) or sg.city,
                "shotgun_event_id": sg.event_id_provider,
            }
            rows.append(row)

    # 2) ajoute les Dice non utilisés
    for dc in dice_events or []:
        if dc.event_id_provider in used_dc:
            continue
        dday = _date_only(dc.event_datetime_local)
        row: Dict[str, Any] = {
            "event_name": dc.event_name or "",
            "event_date": dday.isoformat() if dday else "",
            "shotgun_tickets_sold": None,
            "dice_tickets_sold": dc.tickets_sold_total,
            "artist": getattr(dc, "artist_name", ""),
            "venue": getattr(dc, "venue_name", None) or dc.city,
            "dice_event_id": dc.event_id_provider,
        }
        rows.append(row)

    rows.sort(key=_sort_key)
    return rows
