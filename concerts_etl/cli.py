# concerts_etl/cli.py
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from concerts_etl.adapters import shotgun as shotgun_adapter
from concerts_etl.adapters import dice as dice_adapter
from concerts_etl.core.consolidate_events import consolidate_events
from concerts_etl.core.gsheet import export_to_gsheet

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("concerts_etl.cli")


def _dt_to_str(v: Any) -> Any:
    """
    Sérialise une valeur de date/heure pour JSON.
    - datetime -> isoformat()
    - str -> renvoyée telle quelle
    - autre/None -> renvoyé tel quel
    """
    if isinstance(v, datetime):
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


async def run_all() -> None:
    # 1) Providers
    sg_events = await shotgun_adapter.run()
    dc_events = await dice_adapter.run()

    log.info("Shotgun: %s events", len(sg_events))
    log.info("Dice: %s events", len(dc_events))

    # 2) Consolidation (date = jour, heure ignorée — géré dans consolidate_events)
    rows: List[Dict[str, Any]] = consolidate_events(sg_events, dc_events)

    # 3) Export Google Sheet
    await export_to_gsheet(rows)

    # 4) Petits aperçus JSON pour debug/artefacts
    providers_preview = {
        "shotgun": [
            {
                "name": e.event_name,
                "artist": getattr(e, "artist_name", None),
                "venue": getattr(e, "venue_name", None),
                "dt": _dt_to_str(e.event_datetime_local),
                "tickets": e.tickets_sold_total,
            }
            for e in sg_events[:20]
        ],
        "dice": [
            {
                "name": e.event_name,
                "artist": getattr(e, "artist_name", None),
                "venue": getattr(e, "venue_name", None),
                "dt": _dt_to_str(e.event_datetime_local),
                "tickets": e.tickets_sold_total,
            }
            for e in dc_events[:20]
        ],
        "consolidated": rows[:20],  # déjà str pour event_datetime_local
    }
    with open("providers_preview.json", "w", encoding="utf-8") as f:
        json.dump(providers_preview, f, ensure_ascii=False, indent=2)

    log.info("Done. Consolidated rows: %d", len(rows))


def main() -> None:
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
