import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()
@dataclass(frozen=True)
class Settings:
    shotgun_email: str = os.getenv("SHOTGUN_EMAIL","")
    shotgun_password: str = os.getenv("SHOTGUN_PASSWORD","")
    gsheet_id: str = os.getenv("GSHEET_ID","")
    gsheet_doc_title: str = os.getenv("GSHEET_DOC_TITLE","Concerts Pointages")
    gsheet_worksheet: str = os.getenv("GSHEET_WORKSHEET","shotgun_events")
    export_csv_dir: str = os.getenv("EXPORT_CSV_DIR","exports")
    smtp_host: str = os.getenv("SMTP_HOST","")
    smtp_port: int = int(os.getenv("SMTP_PORT","587"))
    smtp_user: str = os.getenv("SMTP_USER","")
    smtp_pass: str = os.getenv("SMTP_PASS","")
    alert_email_to: str = os.getenv("ALERT_EMAIL_TO","")
    alert_email_from: str = os.getenv("ALERT_EMAIL_FROM","alerts@example.org")
    dice_email: str = os.getenv("DICE_EMAIL","")
    dice_password: str = os.getenv("DICE_PASSWORD","")

settings = Settings()
