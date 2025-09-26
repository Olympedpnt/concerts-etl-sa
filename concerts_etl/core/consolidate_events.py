# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from concerts_etl.core.model import NormalizedEvent  # <- adapte si ton fichier s'appelle models.py

# ----------------------- Normalisation texte -----------------------

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _strip_accents(s).lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s.strip()

def _date_only(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    # on force en UTC pour être stable, puis on ne garde que la date
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).date().isoformat()
    except Exception:
        try:
            return dt.date().isoformat()
        except Exception:
            return None

# ----------------------- Scoring fuzzy -----------------------

def _score_match(sg: NormalizedEvent, dc: NormalizedEvent) -> int:
    score = 0

    # ARTIST
    sg_artist = _norm(sg.artist_name or sg.event_name)
    dc_artist = _norm(dc.artist_name or dc.event_name)
    if sg_artist and dc_artist:
        if sg_artist == dc_artist:
            score += 4
        elif sg_artist in dc_artist or dc_artist in sg_artist:
            score += 2
    else:
        # fallback: l’artiste SG est contenu dans le nom Dice ou inversement
        if sg_artist and _norm(dc.event_name).find(sg_artist) >= 0:
            score += 2
        if dc_artist and _norm(sg.event_name).find(dc_artist) >= 0:
            score += 2

    # VENUE / CITY
    sg_venue = _norm(sg.venue_name) or _norm(sg.city)
    dc_venue = _norm(dc.venue_name) or _norm(dc.city)
    if sg_venue and dc_venue:
        if sg_venue == dc_venue:
            score += 4
        elif sg_venue in dc_venue or dc_venue in sg_venue:
            score += 2

    # DATE (jour)
    sg_d = _date_only(sg.event_datetime_local)
    dc_d = _date_only(dc.event_datetime_local)
    if sg_d and dc_d:
        if sg_d == dc_d:
            score += 3
        else:
            # tolérance ±1 jour (décalages de fuseau / saisie différente)
            try:
                dsg = datetime.fromisoformat(sg_d)
                ddc = datetime.fromisoformat(dc_d)
                delta_days = abs((ddc - dsg).days)
                if delta_days <= 1:
                    score += 2
            except Exception:
                pass

    return score

# ----------------------- Fusion -----------------------

def _to_row(
    sg: Optional[NormalizedEvent],
    dc: Optional[NormalizedEvent]
) -> Dict[str, Any]:
    # colonnes de base + identifiants utiles au debug
    name = (sg.event_name if sg else (dc.event_name if dc else "")).strip()
    when = sg.event_datetime_local if sg else (dc.event_datetime_local if dc else None)
    artist = (sg.artist_name if sg and sg.artist_name else (dc.artist_name if dc else "")) or ""
    venue = (sg.venue_name if sg and sg.venue_name else (dc.venue_name if dc else (sg.city if sg else dc.city if dc else ""))) or ""

    return {
        "event_name": name,
        "event_datetime_local": when,
        "shotgun_tickets_sold": sg.tickets_sold_total if sg else None,
        "dice_tickets_sold": dc.tickets_sold_total if dc else None,
        "artist": artist,
        "venue": venue,
        "shotgun_event_id": sg.event_id_provider if sg else None,
        "dice_event_id": dc.event_id_provider if dc else None,
    }

def _sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
    v = row.get("event_datetime_local")
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
    Jointure fuzzy :
      - on parcourt d'abord *Dice* et on cherche le meilleur SG (score maxi),
        puis on verrouille le SG pour qu'il ne soit apparié qu'une fois.
      - on ajoute ensuite les SG non appariés (colonnes Dice vides).
      - on garde aussi les Dice non appariés si aucun SG n’a été trouvé (colonnes SG vides).
    """
    sg_pool: List[NormalizedEvent] = list(shotgun_events or [])
    dc_pool: List[NormalizedEvent] = list(dice_events or [])
    used_sg: set[int] = set()

    rows: List[Dict[str, Any]] = []

    # 1) Pour chaque Dice, cherche le meilleur Shotgun
    for dc in dc_pool:
        best_i = -1
        best_score = -1
        for i, sg in enumerate(sg_pool):
            if i in used_sg:
                continue
            score = _score_match(sg, dc)
            if score > best_score:
                best_score = score
                best_i = i

        # Seuil raisonnable : 6 (ex: artiste exact 4 + venue proche 2, ou artiste 4 + date 3)
        if best_i >= 0 and best_score >= 6:
            used_sg.add(best_i)
            rows.append(_to_row(sg_pool[best_i], dc))
        else:
            # pas de SG crédible → garder Dice seul
            rows.append(_to_row(None, dc))

    # 2) Ajoute les SG restants non appariés
    for i, sg in enumerate(sg_pool):
        if i not in used_sg:
            rows.append(_to_row(sg, None))

    # 3) Tri final
    rows.sort(key=_sort_key)
    return rows
