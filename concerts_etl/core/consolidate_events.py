# concerts_etl/core/consolidate_events.py
from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from concerts_etl.core.models import NormalizedEvent


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _norm_name(name: Optional[str]) -> str:
    if not name:
        return ""
    s = _strip_accents(name).lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)  # retire ponctuation
    return s.strip()


def _key(ev: NormalizedEvent) -> Tuple[str, Optional[str]]:
    """
    Clé de fusion: (nom normalisé, datetime ISO sans tz si présent).
    On reste strict pour éviter les collisions hasardeuses.
    """
    nm = _norm_name(ev.event_name)
    dt = ev.event_datetime_local.isoformat() if ev.event_datetime_local else None
    return nm, dt


def consolidate_events(
    shotgun_events: List[NormalizedEvent],
    dice_events: List[NormalizedEvent],
) -> List[Dict[str, Any]]:
    """
    Fusionne les événements Shotgun & DICE sur (nom normalisé + datetime local exact).
    Retourne une liste de dicts prêts pour export GSheet.
    """
    sg_map: Dict[Tuple[str, Optional[str]], NormalizedEvent] = {}
    dc_map: Dict[Tuple[str, Optional[str]], NormalizedEvent] = {}

    for ev in shotgun_events or []:
        sg_map[_key(ev)] = ev
    for ev in dice_events or []:
        dc_map[_key(ev)] = ev

    all_keys = set(sg_map.keys()) | set(dc_map.keys())
    rows: List[Dict[str, Any]] = []

    for k in all_keys:
        sg = sg_map.get(k)
        dc = dc_map.get(k)

        # On choisit un nom affiché prioritairement depuis Shotgun, sinon DICE
        event_name = (sg.event_name if sg else (dc.event_name if dc else "")).strip()
        event_dt = sg.event_datetime_local if sg else (dc.event_datetime_local if dc else None)

        row: Dict[str, Any] = {
            "event_name": event_name,
            "event_datetime_local": event_dt,
            "shotgun_tickets_sold": sg.tickets_sold_total if sg else None,
            "dice_tickets_sold": dc.tickets_sold_total if dc else None,
        }

        # Infos utiles pour debug / traçabilité
        if sg:
            row["shotgun_event_id"] = sg.event_id_provider
        if dc:
            row["dice_event_id"] = dc.event_id_provider

        rows.append(row)

    # tri par date puis nom
    rows.sort(key=lambda r: ((r["event_datetime_local"] or ""), r["event_name"] or ""))
    return rows
