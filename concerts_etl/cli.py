from __future__ import annotations

import asyncio, json, logging
from datetime import datetime
from typing import Any, Dict, List

from concerts_etl.adapters import shotgun, dice
from concerts_etl.core.consolidate_events import consolidate_events
from concerts_etl.core.gsheet import export_to_gsheet

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='***"ts": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": "%(message)s"***')

async def run_all():
    # 1) run providers en parallèle
    sg_task = asyncio.create_task(shotgun.run())
    dc_task = asyncio.create_task(dice.run())
    sg_events, dc_events = await asyncio.gather(sg_task, dc_task)

    log.info("Shotgun: %s events", len(sg_events))
    log.info("Dice: %s events", len(dc_events))

    # 2) consolidation (jointure artiste/lieu/date avec tolérance)
    rows: List[Dict[str, Any]] = consolidate_events(sg_events, dc_events)

    # 3) petit aperçu local (utile pour debug Actions)
    with open("providers_preview.json", "w", encoding="utf-8") as f:
        json.dump([
            {
                "event_name": r.get("event_name"),
                "event_datetime_local": (
                    r["event_datetime_local"].isoformat()
                    if isinstance(r.get("event_datetime_local"), datetime) else r.get("event_datetime_local")
                ),
                "artist": r.get("artist"), "venue": r.get("venue"),
                "shotgun_tickets_sold": r.get("shotgun_tickets_sold"),
                "dice_tickets_sold": r.get("dice_tickets_sold"),
            } for r in rows[:20]
        ], f, ensure_ascii=False, indent=2)

    # 4) export Google Sheet (onglet = settings.gsheet_worksheet)
    await export_to_gsheet(rows)

def main():
    asyncio.run(run_all())

if __name__ == "__main__":
    main()
