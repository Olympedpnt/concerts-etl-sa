# concerts_etl/cli.py
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, List, Dict

from concerts_etl.adapters import shotgun, dice
from concerts_etl.core.consolidate_events import consolidate_events
from concerts_etl.core.gsheet import export_to_gsheet

log = logging.getLogger(__name__)


def _json_default(o: Any):
    """
    Fallback JSON encoder:
    - datetime et autres objets → str(o)
    """
    try:
        import datetime as _dt
        if isinstance(o, (_dt.datetime, _dt.date, _dt.time)):
            return o.isoformat()
    except Exception:
        pass
    return str(o)


async def run_all() -> None:
    # 1) Collecte providers
    log.info("Collecte Shotgun…")
    sg_events = await shotgun.run()

    log.info("Collecte DICE…")
    dc_events = await dice.run()

    # 2) Consolidation (fusion sur nom + datetime local)
    consolidated: List[Dict[str, Any]] = consolidate_events(sg_events, dc_events)

    # 3) Écrit un petit aperçu JSON local (pour debug artefacts)
    try:
        with open("providers_preview.json", "w", encoding="utf-8") as f:
            json.dump(consolidated[:20], f, ensure_ascii=False, indent=2, default=_json_default)
    except Exception as e:
        log.warning(f"Preview JSON writing failed: {e}")

    # 4) Export Google Sheet
    try:
        await export_to_gsheet(consolidated)
        log.info("Export Google Sheet terminé.")
    except Exception as e:
        log.error(f"Export Google Sheet échoué: {e}")

    # 5) Log final
    log.info(
        f"Collecte terminée — Shotgun={len(sg_events)} / Dice={len(dc_events)} / Consolidés={len(consolidated)}"
    )


def main() -> None:
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
