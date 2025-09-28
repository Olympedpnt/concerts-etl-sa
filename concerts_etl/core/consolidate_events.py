# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Set

from concerts_etl.core.models import NormalizedEvent

# ------------------- normalisation / tokenisation -------------------

_STOPWORDS = {
    "the","and","feat","ft","with","x","&","+","-", "–","—",
    "le","la","les","l","de","du","des","et","au","aux","chez","a","an","on","in",
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

def _date_str(e: Optional[NormalizedEvent]) -> str:
    if not e or not e.event_datetime_local:
        return ""
    v = e.event_datetime_local
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, str):
        m = re.match(r"(\d{4}-\d{2}-\d{2})", v)
        return m.group(1) if m else v
    return ""

def _artist_tokens(*fields: Optional[str]) -> Set[str]:
    """
    Découpe artistes / noms d’event en tokens utiles (accents enlevés).
    Séparateurs: , ; / + & x - – — feat ft with @
    Filtre les tokens courts et stopwords.
    """
    tokens: Set[str] = set()
    for raw in fields:
        if not raw:
            continue
        s = _norm_basic(raw)
        # uniformiser les séparateurs multi-artistes en virgule
        s = re.sub(r"\b(feat|ft|with)\b", ",", s)
        s = re.sub(r"\s+[xX]\s+", ",", s)
        s = s.replace("&", ",").replace("+", ",").replace("/", ",").replace(" @ ", ",")
        s = s.replace(" – ", ",").replace(" — ", ",").replace(" - ", ",")
        # découpe
        parts = []
        for chunk in s.split(","):
            chunk = re.sub(r"[^\w\s]", " ", chunk).strip()
            if chunk:
                parts.extend(chunk.split())
        for t in parts:
            if len(t) <= 2:
                continue
            if t in _STOPWORDS:
                continue
            tokens.add(t)
    return tokens

# ------------------- tri / sortie -------------------

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    dt = row.get("event_datetime_local") or row.get("event_date") or ""
    nm = (row.get("event_name") or "").lower()
    return str(dt), nm

# ------------------- consolidation -------------------

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusionne par DATE (jour) + recouvrement d’artistes basé sur tokens.
    - Côté SG: tokens = artist_name + event_name
    - Côté DICE: tokens = artist_name + event_name
    - Match si intersection(tokens) >= 1
    - On choisit le meilleur match (max overlap) quand plusieurs SG sont éligibles
    """
    # Index SG par jour -> liste (sg, tokens)
    sg_by_day: Dict[str, List[Tuple[NormalizedEvent, Set[str]]]] = {}
    for sg in (shotgun_events or []):
        d = _date_str(sg)
        if not d:
            continue
        toks = _artist_tokens(getattr(sg, "artist_name", None), sg.event_name)
        # même si vide, on stocke (mais il matchera rarement)
        sg_by_day.setdefault(d, []).append((sg, toks))

    used_sg: Set[str] = set()
    used_dc: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    # 1) Pour chaque DICE, tente de matcher avec un SG du même jour via tokens
    for dc in (dice_events or []):
        d = _date_str(dc)
        if not d:
            continue
        dc_toks = _artist_tokens(getattr(dc, "artist_name", None), dc.event_name)

        best: Optional[Tuple[NormalizedEvent, Set[str], int]] = None  # (sg, toks, overlap_count)
        for sg, sg_toks in sg_by_day.get(d, []):
            if sg.event_id_provider in used_sg:
                continue
            overlap = len(dc_toks & sg_toks) if dc_toks and sg_toks else 0
            if overlap > 0:
                if best is None or overlap > best[2]:
                    best = (sg, sg_toks, overlap)

        if best:
            sg, _, _ = best
            used_sg.add(sg.event_id_provider)
            used_dc.add(dc.event_id_provider)

            # Champs lisibles
            event_name = sg.event_name or dc.event_name or ""
            artist_disp = (sg.artist_name or getattr(dc, "artist_name", "") or "")
            venue_disp = (
                getattr(sg, "venue_name", None)
                or getattr(dc, "venue_name", None)
                or sg.city
                or dc.city
                or ""
            )

            rows.append({
                "event_name": event_name,
                "event_datetime_local": d,  # date uniquement
                "artist": artist_disp,
                "venue": venue_disp,
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": dc.tickets_sold_total,
                "shotgun_event_id": sg.event_id_provider,
                "dice_event_id": dc.event_id_provider,
            })

    # 2) SG non appariés
    for sg in (shotgun_events or []):
        if sg.event_id_provider in used_sg:
            continue
        d = _date_str(sg)
        rows.append({
            "event_name": sg.event_name or "",
            "event_datetime_local": d,
            "artist": sg.artist_name or "",
            "venue": getattr(sg, "venue_name", None) or sg.city or "",
            "shotgun_tickets_sold": sg.tickets_sold_total,
            "dice_tickets_sold": None,
            "shotgun_event_id": sg.event_id_provider,
        })

    # 3) DICE non appariés
    for dc in (dice_events or []):
        if dc.event_id_provider in used_dc:
            continue
        d = _date_str(dc)
        rows.append({
            "event_name": dc.event_name or "",
            "event_datetime_local": d,
            "artist": getattr(dc, "artist_name", "") or "",
            "venue": getattr(dc, "venue_name", None) or dc.city or "",
            "shotgun_tickets_sold": None,
            "dice_tickets_sold": dc.tickets_sold_total,
            "dice_event_id": dc.event_id_provider,
        })

    rows.sort(key=_sort_key)
    return rows
