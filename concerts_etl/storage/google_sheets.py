import os, csv, logging
from datetime import datetime, timezone
from typing import Iterable, List
import gspread
from google.oauth2.service_account import Credentials
from concerts_etl.core.config import settings
from concerts_etl.core.models import NormalizedEvent
from concerts_etl.core.matching import ConsolidatedRow

log = logging.getLogger(__name__)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def _client():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS introuvable (fichier JSON Service Account manquant)")
    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return gspread.authorize(creds)

def upsert_rows(events: Iterable[NormalizedEvent]) -> str:
    """Append-only dans Google Sheets (historisation journalière)."""
    events = list(events)
    if not events:
        return ""

    gc = _client()
    # Ouvrir le spreadsheet
    if settings.gsheet_id:
        sh = gc.open_by_key(settings.gsheet_id)
    else:
        try:
            sh = gc.open(settings.gsheet_doc_title)
        except gspread.SpreadsheetNotFound:
            sh = gc.create(settings.gsheet_doc_title)

    # Onglet
    try:
        ws = sh.worksheet(settings.gsheet_worksheet)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(settings.gsheet_worksheet, rows=2000, cols=30)

    header = [
    "provider","event_id_provider","event_name","city","country",
    "event_datetime_local","timezone","status","tickets_sold_total",
    "gross_total","net_total","currency","sell_through_pct",
    "scrape_ts_utc","ingestion_run_id"
]


    existing = ws.get_all_values()
    if not existing or existing[0] != header:
        ws.clear()
        ws.append_row(header)

    rows: List[list] = []
    for e in events:
            rows.append([
        e.provider,
        e.event_id_provider,
        e.event_name,
        e.city,
        e.country,
        e.event_datetime_local.isoformat() if e.event_datetime_local else "",
        e.timezone,
        e.status,
        e.tickets_sold_total,
        e.gross_total,
        e.net_total,
        e.currency,
        e.sell_through_pct,
        e.scrape_ts_utc.isoformat(),
        e.ingestion_run_id,
    ])


    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info("gsheets.appended", extra={"count": len(rows), "sheet": sh.id})
    return sh.id

def export_csv(events: Iterable[NormalizedEvent], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"shotgun_{datetime.now(timezone.utc).date()}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "provider","event_id_provider","event_name","city","country",
            "event_datetime_local","timezone","status","tickets_sold_total",
            "gross_total","net_total",
            "currency","sell_through_pct","scrape_ts_utc","ingestion_run_id"
        ])
        for e in events:
            w.writerow([
                e.provider, e.event_id_provider, e.event_name, e.city, e.country,
                e.event_datetime_local.isoformat() if e.event_datetime_local else "",
                e.timezone, e.status, e.tickets_sold_total,
                e.gross_total, e.net_total, e.currency,
                e.sell_through_pct, e.scrape_ts_utc.isoformat(), e.ingestion_run_id
            ])
    return path

def upsert_rows_consolidated(rows: Iterable[ConsolidatedRow]) -> str:
    rows = list(rows)
    if not rows:
        return ""
    gc = _client()
    sh = gc.open_by_key(settings.gsheet_id) if settings.gsheet_id else gc.open(settings.gsheet_doc_title)
    try:
        ws = sh.worksheet(settings.gsheet_worksheet)
    except Exception:
        ws = sh.add_worksheet(settings.gsheet_worksheet, rows=2000, cols=30)

    header = [
        "canonical_event_key","event_name","event_datetime_local","timezone",
        "tickets_sold_total_shotgun","tickets_sold_total_dice",
        "scrape_ts_utc","ingestion_run_id",
    ]
    existing = ws.get_all_values()
    if not existing or existing[0] != header:
        if existing: ws.clear()
        ws.append_row(header)

    data = []
    for r in rows:
        data.append([
            r.canonical_event_key,
            r.event_name,
            r.event_datetime_local.isoformat() if r.event_datetime_local else "",
            r.timezone,
            r.tickets_sold_total_shotgun,
            r.tickets_sold_total_dice,
            r.scrape_ts_utc.isoformat(),
            r.ingestion_run_id,
        ])
    if data:
        ws.append_rows(data, value_input_option="USER_ENTERED")
    return sh.id

def export_csv_consolidated(rows: Iterable[ConsolidatedRow], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"consolidated_{datetime.now(timezone.utc).date()}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "canonical_event_key","event_name","event_datetime_local","timezone",
            "tickets_sold_total_shotgun","tickets_sold_total_dice",
            "scrape_ts_utc","ingestion_run_id",
        ])
        for r in rows:
            w.writerow([
                r.canonical_event_key,
                r.event_name,
                r.event_datetime_local.isoformat() if r.event_datetime_local else "",
                r.timezone,
                r.tickets_sold_total_shotgun,
                r.tickets_sold_total_dice,
                r.scrape_ts_utc.isoformat(),
                r.ingestion_run_id,
            ])
    return path


__all__ = ["upsert_rows", "export_csv"]
