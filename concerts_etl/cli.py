# concerts_etl/cli.py
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List

from concerts_etl.adapters.shotgun import run as run_shotgun
from concerts_etl.adapters.dice import run as run_dice
from concerts_etl.core.consolidate_events import consolidate_events
from concerts_etl.core.gsheet import export_to_gsheet

log = logging.getLogger("concerts_etl.cli")
logging.basicConfig(level=logging.INFO)


async def run_all():
    # --- Shotgun ---
    sg_events = []
    try:
        sg_events = await run_shotgun()
    except Exception:
        log.exception("Shotgun adapter failed")

    # --- Dice ---
    dc_events = []
    try:
        dc_events = await run_dice()
    except Exception:
        log.exception("Dice adapter failed")

    log.info("Shotgun: %d events", len(sg_events))
    log.info("Dice: %d events", len(dc_events))

    # --- Consolidation ---
    rows: List[Dict[str, Any]] = consolidate_events(sg_events, dc_events)

    # --- Prévisualisation locale ---
    with open("providers_preview.json", "w", encoding="utf-8") as f:
        json.dump(
            [
                {
                    **r,
                    "event_datetime_local": (
                        r["event_datetime_local"].isoformat()
                        if r.get("event_datetime_local") else None
                    )
                }
                for r in rows[:20]
            ],
            f, ensure_ascii=False, indent=2
        )

    # --- Export vers Google Sheet ---
    await export_to_gsheet(rows)


def main():
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
