import asyncio, logging, sys
from concerts_etl.core.logging import configure_logging
from concerts_etl.storage.google_sheets import upsert_rows_consolidated, export_csv_consolidated
from concerts_etl.adapters import REGISTRY
from concerts_etl.core.matching import merge_shotgun_dice

configure_logging()

async def run_all() -> int:
    # 1) collecter
    sg = await REGISTRY["shotgun"]()
    dc = await REGISTRY["dice"]()

    # comptage & exemples (logs)
    logging.info("collected", extra={"shotgun": len(sg), "dice": len(dc)})
    try:
        eg = {"shotgun": [(e.event_name, e.tickets_sold_total) for e in sg[:3]],
              "dice": [(e.event_name, e.tickets_sold_total) for e in dc[:3]]}
        with open("providers_preview.json", "w", encoding="utf-8") as f:
            import json; json.dump(eg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # 2) fusionner
    rows = merge_shotgun_dice(sg, dc, hour_tolerance_min=30, name_threshold=0.90)
    if not rows:
        logging.warning("no_data"); return 1
    # 3) exporter
    upsert_rows_consolidated(rows)
    export_csv_consolidated(rows, "exports")
    logging.info("done", extra={"count": len(rows), "providers": ["shotgun","dice"]})
    return 0

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd in ("run","all"):
        raise SystemExit(asyncio.run(run_all()))
    print("Usage: python -m concerts_etl run"); raise SystemExit(2)

if __name__ == "__main__":
    main()
