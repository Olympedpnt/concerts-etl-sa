from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from concerts_etl.core.gsheet import export_to_gsheet
from concerts_etl.core.consolidate_events import consolidate_events
from concerts_etl.core.models import NormalizedEvent

# On importe les modules d'adapters directement, sans passer par adapters/__init__.py
from concerts_etl.adapters import shotgun as shotgun_adapter
from concerts_etl.adapters import dice as dice_adapter

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

async def run_all() -> None:
    # 1) Shotgun
    try:
        sg_events: List[NormalizedEvent] = await shotgun_adapter.run()
    except Exception as e:
        log.exception("Shotgun: échec run()")
        sg_events = []
    log.info("Shotgun: %s events", len(sg_events))

    # 2) DICE
    try:
        dc_events: List[NormalizedEvent] = await dice_adapter.run()
    except Exception as e:
        log.exception("Dice: échec run()")
        dc_events = []
    log.info("Dice: %s events", len(dc_events))

    # 3) Consolidation (date-only + règles de matching)
    rows: List[Dict[str, Any]] = consolidate_events(sg_events, dc_events)

    # 4) Export gsheet
    await export_to_gsheet(rows)

    # 5) Dump providers preview pour debug local (dates -> string)
    try:
        preview = []
        for r in rows[:20]:
            r2 = dict(r)
            v = r2.get("event_datetime_local")
            if hasattr(v, "isoformat"):
                r2["event_datetime_local"] = v.isoformat()
            preview.append(r2)
        with open("providers_preview.json", "w", encoding="utf-8") as f:
            json.dump(preview, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def main() -> None:
    asyncio.run(run_all())

if __name__ == "__main__":
    main()
