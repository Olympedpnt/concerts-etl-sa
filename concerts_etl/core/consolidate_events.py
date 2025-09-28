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
    Extrait des tokens d'artistes depuis artist_name ET event_name.
    Séparateurs gérés: , ; / + & x - – — feat ft with @
    Filtre tokens courts et stopwords.
    """
    tokens: Set[str] = set()
    for raw in fields:
        if not raw:
            continue
        s = _norm_basic(raw)
        s = re.sub(r"\b(feat|ft|with)\b", ",", s)
        s = re.sub(r"\s+[xX]\s+", ",", s)
        s = s.replace("&", ",").replace("+", ",").replace("/", ",").replace(" @ ", ",")
        s = s.replace(" – ", ",").replace(" — ", ",").replace(" - ", ",")
        parts: List[str] = []
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
    Règle principale : fusion par DATE (jour) + recouvrement d’artistes (tokens).
    Fallback important : si la date Shotgun est absente, on essaie quand même de
    matcher un DICE du même jour via tokens, et on prend alors la date de DICE.
    """
    # Index SG par jour + SG sans date
    sg_by_day: Dict[str, List[Tuple[NormalizedEvent, Set[str]]]] = {}
    sg_no_date: List[Tuple[NormalizedEvent, Set[str]]] = []

    for sg in (shotgun_events or []):
        d = _date_str(sg)
        toks = _artist_tokens(getattr(sg, "artist_name", None), sg.event_name)
        if d:
            sg_by_day.setdefault(d, []).append((sg, toks))
        else:
            sg_no_date.append((sg, toks))

    used_sg: Set[str] = set()
    used_dc: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    # 1) Pour chaque DICE, tente d’abord un match sur le même jour (SG avec date)
    #    puis fallback sur SG sans date (en reprenant la date de DICE).
    for dc in (dice_events or []):
        d = _date_str(dc)
        if not d:
            continue
        dc_toks = _artist_tokens(getattr(dc, "artist_name", None), dc.event_name)

        best: Optional[Tuple[NormalizedEvent, int, bool]] = None
        # bool = True si match dans sg_by_day (avec date), False si sg_no_date

        # a) match sur même jour
        for sg, sg_toks in sg_by_day.get(d, []):
            if sg.event_id_provider in used_sg:
                continue
            overlap = len(dc_toks & sg_toks) if dc_toks and sg_toks else 0
            if overlap > 0 and (best is None or overlap > best[1]):
                best = (sg, overlap, True)

        # b) fallback : SG sans date → si overlap, on reprendra la date de DICE
        if best is None:
            for sg, sg_toks in sg_no_date:
                if sg.event_id_provider in used_sg:
                    continue
                overlap = len(dc_toks & sg_toks) if dc_toks and sg_toks else 0
                if overlap > 0 and (best is None or overlap > best[1]):
                    best = (sg, overlap, False)

        if best:
            sg, _overlap, from_with_date = best
            used_sg.add(sg.event_id_provider)
            used_dc.add(dc.event_id_provider)

            event_name = sg.event_name or dc.event_name or ""
            artist_disp = (sg.artist_name or getattr(dc, "artist_name", "") or "")
            venue_disp = (
                getattr(sg, "venue_name", None)
                or getattr(dc, "venue_name", None)
                or sg.city
                or dc.city
                or ""
            )
            # si SG n’a pas de date → on prend celle de DICE
            day_str = _date_str(sg) if from_with_date else d

            rows.append({
                "event_name": event_name,
                "event_datetime_local": day_str,   # YYYY-MM-DD
                "artist": artist_disp,
                "venue": venue_disp,
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": dc.tickets_sold_total,
                "shotgun_event_id": sg.event_id_provider,
                "dice_event_id": dc.event_id_provider,
            })

    # 2) SG non appariés (avec et sans date)
    for sg, _ in [(s, t) for lst in sg_by_day.values() for (s, t) in lst] + sg_no_date:
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
