# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple, Set

from concerts_etl.core.models import NormalizedEvent

# ------------------- normalisation / tokenisation -------------------

_STOPWORDS = {
    "the","and","feat","ft","with","x","&","+","-","–","—",
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

def _date_str(e: Optional[NormalizedEvent]) -> str:
    """Retourne la date jour 'YYYY-MM-DD' (heure ignorée)."""
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
            if len(t) <= 2 or t in _STOPWORDS:
                continue
            tokens.add(t)
    return tokens

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    """Tri ascendant par date (YYYY-MM-DD)."""
    dt = row.get("event_datetime_local") or ""
    nm = (row.get("event_name") or "").lower()
    return dt, nm

# ------------------- consolidation -------------------

def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusion par DATE (jour) + recouvrement de tokens d’artiste.
    - Si pas de date côté SG, l’event est exclu.
    - On filtre les dates passées.
    """
    sg_by_day: Dict[str, List[Tuple[NormalizedEvent, Set[str]]]] = {}
    for sg in (shotgun_events or []):
        d = _date_str(sg)
        if not d:
            continue  # exclut SG sans date
        toks = _artist_tokens(getattr(sg, "artist_name", None), sg.event_name)
        sg_by_day.setdefault(d, []).append((sg, toks))

    used_sg: Set[str] = set()
    used_dc: Set[str] = set()
    rows: List[Dict[str, Any]] = []

    # apparier DICE -> SG
    for dc in (dice_events or []):
        d = _date_str(dc)
        if not d:
            continue
        dc_toks = _artist_tokens(getattr(dc, "artist_name", None), dc.event_name)

        best: Optional[Tuple[NormalizedEvent, int]] = None

        for sg, sg_toks in sg_by_day.get(d, []):
            if sg.event_id_provider in used_sg:
                continue
            overlap = len(dc_toks & sg_toks)
            if overlap > 0 and (best is None or overlap > best[1]):
                best = (sg, overlap)

        if best:
            sg, _ = best
            used_sg.add(sg.event_id_provider)
            used_dc.add(dc.event_id_provider)

            event_name = sg.event_name or dc.event_name or ""
            artist_disp = sg.artist_name or getattr(dc, "artist_name", "") or ""
            venue_disp = (
                getattr(sg, "venue_name", None)
                or getattr(dc, "venue_name", None)
                or sg.city
                or dc.city
                or ""
            )

            rows.append({
                "event_name": event_name,
                "event_datetime_local": d,
                "artist": artist_disp,
                "venue": venue_disp,
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": dc.tickets_sold_total,
                "shotgun_event_id": sg.event_id_provider,
                "dice_event_id": dc.event_id_provider,
            })

    # SG restants
    for d, lst in sg_by_day.items():
        for sg, _ in lst:
            if sg.event_id_provider in used_sg:
                continue
            rows.append({
                "event_name": sg.event_name or "",
                "event_datetime_local": d,
                "artist": sg.artist_name or "",
                "venue": getattr(sg, "venue_name", None) or sg.city or "",
                "shotgun_tickets_sold": sg.tickets_sold_total,
                "dice_tickets_sold": None,
                "shotgun_event_id": sg.event_id_provider,
            })

    # DICE restants
    for dc in (dice_events or []):
        if dc.event_id_provider in used_dc:
            continue
        d = _date_str(dc)
        if not d:
            continue
        rows.append({
            "event_name": dc.event_name or "",
            "event_datetime_local": d,
            "artist": getattr(dc, "artist_name", "") or "",
            "venue": getattr(dc, "venue_name", None) or dc.city or "",
            "shotgun_tickets_sold": None,
            "dice_tickets_sold": dc.tickets_sold_total,
            "dice_event_id": dc.event_id_provider,
        })

    # filtre dates passées
    today = date.today().isoformat()
    rows = [r for r in rows if r.get("event_datetime_local") and r["event_datetime_local"] >= today]

    rows.sort(key=_sort_key)
    return rows
