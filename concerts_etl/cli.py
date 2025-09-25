# concerts_etl/cli.py
import asyncio
import logging
from typing import List, Dict, Any

from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.gsheet import export_to_gsheet
from concerts_etl.adapters.shotgun import run as run_shotgun
from concerts_etl.adapters.dice import run as run_dice

log = logging.getLogger(__name__)


async def consolidate_events(shotgun_events: List[NormalizedEvent], dice_events: List[NormalizedEvent]) -> List[Dict[str, Any]]:
    """
    Fusionne les événements Shotgun et Dice sur (nom + datetime).
    Retourne une liste de dicts prêts à être envoyés vers Google Sheets.
    """
    consolidated: Dict[tuple, Dict[str, Any]] = {}

    # Normalisation clé (nom + date locale en iso)
    def make_key(ev: NormalizedEvent):
        return (
            (ev.event_name or "").strip().lower(),
            ev.event_datetime_local.isoformat() if ev.event_datetime_local else None,
        )

    # Shotgun d’abord
    for ev in shotgun_events:
        k = make_key(ev)
        consolidated[k] = {
            "event_name": ev.event_name,
            "event_datetime_local": ev.event_datetime_local,
            "shotgun_tickets_sold": ev.tickets_sold_total,
            "dice_tickets_sold": None,
        }

    # Merge Dice
    for ev in dice_events:
        k = make_key(ev)
        if k in consolidated:
            consolidated[k]["dice_tickets_sold"] = ev.tickets_sold_total
        else:
            consolidated[k] = {
                "event_name": ev.event_name,
                "event_datetime_local": ev.event_datetime_local,
                "shotgun_tickets_sold": None,
                "dice_tickets_sold": ev.tickets_sold_total,
            }

    return list(consolidated.values())


async def run_all():
    log.info("Démarrage de l’ETL concerts")

    # Lancer les 2 providers
    log.info("→ Shotgun")
    shotgun_events = await run_shotgun()
    log.info(f"Shotgun: {len(shotgun_events)} événements")

    log.info("→ Dice")
    dice_events = await run_dice()
    log.info(f"Dice: {len(dice_events)} événements")

    # Consolidation
    consolidated = await consolidate_events(shotgun_events, dice_events)
    log.info(f"Consolidé: {len(consolidated)} lignes")

    # Export Google Sheet
    await export_to_gsheet(consolidated)

    # En option : écrire un preview local
    import json
    with open("providers_preview.json", "w", encoding="utf-8") as f:
        json.dump(consolidated[:20], f, ensure_ascii=False, indent=2)


def main():
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
