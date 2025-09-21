import os, json, sys, time, traceback
from pathlib import Path

def log(msg): print(f"[DIAG] {msg}", flush=True)

def mask(v, keep=3):
    if not v: return "<empty>"
    return v[:keep] + "…" + str(len(v))

def main():
    rc = 0

    # A) ENV de base
    log("Python: " + sys.version)
    for k in ["SHOTGUN_EMAIL","SHOTGUN_PASSWORD","GSHEET_ID","GOOGLE_APPLICATION_CREDENTIALS"]:
        v = os.getenv(k, "")
        log(f"env {k}: {mask(v)}")

    # B) Fichier credentials Google
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS","")
    try:
        p = Path(creds_path)
        log(f"Creds path exists? {p.exists()} size={p.stat().st_size if p.exists() else 0}")
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            log("Creds JSON ok, client_email=" + data.get("client_email","<none>"))
        else:
            log("ERROR: missing GOOGLE_APPLICATION_CREDENTIALS")
            rc = 1
    except Exception as e:
        log("ERROR: reading creds: " + repr(e))
        rc = 1

    # C) Import package
    try:
        import concerts_etl
        log("import concerts_etl OK: " + (getattr(concerts_etl, "__file__", "<pkg>") or "<pkg>"))
    except Exception:
        log("ERROR: cannot import concerts_etl")
        traceback.print_exc()
        rc = 1

    # D) Test Google Sheets append
    try:
        from google.oauth2.service_account import Credentials
        import gspread
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sid = os.getenv("GSHEET_ID","")
        if not sid:
            log("WARN: GSHEET_ID empty → ouverture par titre")
            sh = gc.open(os.getenv("GSHEET_DOC_TITLE","Concerts Pointages"))
        else:
            sh = gc.open_by_key(sid)
        try:
            ws = sh.worksheet(os.getenv("GSHEET_WORKSHEET","shotgun_events"))
        except Exception:
            ws = sh.add_worksheet(os.getenv("GSHEET_WORKSHEET","shotgun_events"), rows=1000, cols=30)
        row = ["DIAG", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "ok"]
        ws.append_row(row, value_input_option="USER_ENTERED")
        log("Sheets append OK (ligne DIAG ajoutée)")
    except Exception:
        log("ERROR: Sheets append failed")
        traceback.print_exc()
        rc = 1

    sys.exit(rc)

if __name__ == "__main__":
    main()
