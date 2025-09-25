# concerts_etl/core/consolidate_events.py
import unicodedata
from collections import defaultdict

def _normalize(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).lower().strip()

def consolidate_events(events):
    """
    Fusionne les événements venant de plusieurs providers (Shotgun, Dice)
    sur la base du nom + date locale.
    """
    merged = defaultdict(dict)

    for ev in events:
        key = (_normalize(ev.event_name), ev.event_datetime_local)

        if "event_name" not in merged[key]:
            merged[key]["event_name"] = ev.event_name
            merged[key]["event_datetime_local"] = ev.event_datetime_local

        if ev.provider == "shotgun":
            merged[key]["shotgun_tickets_sold"] = ev.tickets_sold_total
        elif ev.provider == "dice":
            merged[key]["dice_tickets_sold"] = ev.tickets_sold_total

    return list(merged.values())
