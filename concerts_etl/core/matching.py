from __future__ import annotations
import re, unicodedata
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Optional
from pydantic import BaseModel
from concerts_etl.core.models import NormalizedEvent

# ---- clé canonique & normalisation ----

STOPWORDS = {"live","concert","tour"}
def _norm_name(s: str) -> str:
    s = (s or "").lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[\W_]+", " ", s)
    tokens = [t for t in s.split() if t and t not in STOPWORDS]
    return " ".join(tokens)

def _round5(dt: datetime) -> datetime:
    if not dt: return dt
    m = (dt.minute // 5) * 5
    return dt.replace(minute=m, second=0, microsecond=0)

def canonical_key(name: str, dt: Optional[datetime]) -> str:
    nn = _norm_name(name)
    ts = _round5(dt).strftime("%Y-%m-%dT%H:%M") if dt else "na"
    return f"{nn}|{ts}"

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, _norm_name(a), _norm_name(b)).ratio()

# ---- modèle consolidé (une ligne par concert) ----

class ConsolidatedRow(BaseModel):
    canonical_event_key: str
    event_name: str
    event_datetime_local: Optional[datetime]
    timezone: str = "Europe/Paris"
    tickets_sold_total_shotgun: Optional[int] = None
    tickets_sold_total_dice: Optional[int] = None
    scrape_ts_utc: datetime
    ingestion_run_id: str

# ---- fusion shotgun <-> dice ----

def merge_shotgun_dice(shotgun: List[NormalizedEvent], dice: List[NormalizedEvent],
                       hour_tolerance_min: int = 30, name_threshold: float = 0.90) -> List[ConsolidatedRow]:
    out: Dict[str, ConsolidatedRow] = {}

    # index SG par clé
    sg_index: Dict[str, NormalizedEvent] = {}
    for ev in shotgun:
        key = canonical_key(ev.event_name, ev.event_datetime_local)
        sg_index[key] = ev
        out[key] = ConsolidatedRow(
            canonical_event_key=key,
            event_name=ev.event_name,
            event_datetime_local=ev.event_datetime_local,
            tickets_sold_total_shotgun=ev.tickets_sold_total,
            scrape_ts_utc=ev.scrape_ts_utc,
            ingestion_run_id=ev.ingestion_run_id,
        )

    # rattacher DICE à la meilleure clé SG
    for dv in dice:
        best_key = None
        best_score = 0.0
        for key, sv in sg_index.items():
            # même jour
            if sv.event_datetime_local and dv.event_datetime_local and sv.event_datetime_local.date() != dv.event_datetime_local.date():
                continue
            # tolérance horaire
            if sv.event_datetime_local and dv.event_datetime_local:
                if abs((sv.event_datetime_local - dv.event_datetime_local).total_seconds()) > hour_tolerance_min * 60:
                    continue
            # similarité nom
            score = _sim(sv.event_name, dv.event_name)
            if score >= name_threshold and score > best_score:
                best_key, best_score = key, score

        if best_key:
            row = out[best_key]
            row.tickets_sold_total_dice = dv.tickets_sold_total
            # mettre le nom/date si SG manquait (peu probable)
            if not row.event_name: row.event_name = dv.event_name
            if not row.event_datetime_local: row.event_datetime_local = dv.event_datetime_local
        else:
            # pas de SG correspondant → ligne indépendante (colonne SG vide)
            key = canonical_key(dv.event_name, dv.event_datetime_local)
            out[key] = ConsolidatedRow(
                canonical_event_key=key,
                event_name=dv.event_name,
                event_datetime_local=dv.event_datetime_local,
                tickets_sold_total_dice=dv.tickets_sold_total,
                scrape_ts_utc=dv.scrape_ts_utc,
                ingestion_run_id=dv.ingestion_run_id,
            )

    return list(out.values())
