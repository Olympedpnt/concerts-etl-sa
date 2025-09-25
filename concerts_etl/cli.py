from __future__ import annotations
import asyncio, json, os, logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from concerts_etl.adapters import REGISTRY
from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.config import settings

log = logging.getLogger(__name__)

async def _run_provider(name: str) -> List[NormalizedEvent]:
    try:
        log.info(f"run_provider.start name={name}")
        events = await REGISTRY[name]()
        log.info(f"run_provider.ok name={name} count={len(events)}")
        return events
    except Exception as e:
        log.exception(f"run_provider.fail name={name} error={e}")
        return []  # tolérance: on continue avec les autres

async def run_all() -> int:
    # Lance tous les providers en parallèle
    tasks = [asyncio.create_task(_run_provider(name)) for name in REGISTRY.keys()]
    results = await asyncio.gather(*tasks)

    # Aplatir
    all_events: List[NormalizedEvent] = [e for chunk in results for e in chunk]

    # Aperçu par provider
    by_provider: Dict[str, int] = {}
    for ev in all_events:
        by_provider[ev.provider] = by_provider.get(ev.provider, 0) + 1

    # Dump d’un petit preview multi-providers
    preview = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "counts": by_provider,
        "total": len(all_events),
        "sample": [
            {
                "provider": ev.provider,
                "name": ev.event_name,
                "event_id_provider": ev.event_id_provider,
                "dt": ev.event_datetime_local.isoformat() if ev.event_datetime_local else None,
                "sold": ev.tickets_sold_total,
            }
            for ev in all_events[:10]
        ],
    }
    try:
        with open("providers_preview.json", "w", encoding="utf-8") as f:
            json.dump(preview, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # Export CSV optionnel (dossier paramétrable)
    export_dir = settings.export_csv_dir or "exports"
    os.makedirs(export_dir, exist_ok=True)
    try:
        import csv
        path = os.path.join(export_dir, f"consolidated_{datetime.now().date()}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "provider","event_id_provider","event_name","city","country",
                "event_datetime_local","timezone","status","tickets_sold_total",
                "gross_total","net_total","currency","sell_through_pct",
                "scrape_ts_utc","ingestion_run_id"
            ])
            for ev in all_events:
                w.writerow([
                    ev.provider,
                    ev.event_id_provider,
                    ev.event_name,
                    ev.city or "",
                    ev.country or "",
                    ev.event_datetime_local.isoformat() if ev.event_datetime_local else "",
                    ev.timezone or "",
                    ev.status or "",
                    ev.tickets_sold_total if ev.tickets_sold_total is not None else "",
                    ev.gross_total if ev.gross_total is not None else "",
                    ev.net_total if ev.net_total is not None else "",
                    ev.currency or "",
                    ev.sell_through_pct if ev.sell_through_pct is not None else "",
                    ev.scrape_ts_utc.isoformat() if ev.scrape_ts_utc else "",
                    ev.ingestion_run_id or "",
                ])
        log.info(f"export.csv path={path}")
    except Exception:
        log.warning("export.csv.failed", exc_info=True)

    # Ici, si tu as une écriture Google Sheets, fais-la **avec all_events**,
    # et surtout pas filtrée sur provider="shotgun".
    # Exemple (si tu as une fonction dédiée):
    # try:
    #     from concerts_etl.core.gsheet import upsert_events
    #     upsert_events(all_events, sheet_id=settings.gsheet_id,
    #                   doc_title=settings.gsheet_doc_title,
    #                   worksheet=settings.gsheet_worksheet)
    # except Exception:
    #     log.warning("gsheet.upsert.failed", exc_info=True)

    if len(all_events) == 0:
        log.warning("no_data")
        return 1
    log.info("collected")
    return 0

def main() -> None:
    exit(asyncio.run(run_all()))
