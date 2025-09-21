import asyncio, logging, sys
from concerts_etl.core.logging import configure_logging
from concerts_etl.storage.google_sheets import upsert_rows, export_csv
configure_logging()
async def run_shotgun() -> int:
    from concerts_etl.adapters import shotgun
    events = await shotgun.run()
    if not events:
        logging.warning("no_data"); return 1
    upsert_rows(events); export_csv(events, "exports"); logging.info("done", extra={"count":len(events)}); return 0
def main():
    cmd = sys.argv[1] if len(sys.argv)>1 else "run"
    if cmd in ("run","shotgun"): raise SystemExit(asyncio.run(run_shotgun()))
    print("Usage: python -m concerts_etl run"); raise SystemExit(2)
if __name__ == "__main__": main()
