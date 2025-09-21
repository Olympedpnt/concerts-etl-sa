from pydantic import BaseModel, ConfigDict
from typing import Optional, Literal
from datetime import datetime
class RawShotgunCard(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)
    event_id_provider: str; event_name: str
    event_datetime_local: Optional[datetime] = None
    city: Optional[str] = None; country: Optional[str] = None
    gross_total: Optional[float] = None; gross_today: Optional[float] = None
    tickets_sold_total: Optional[int] = None; sell_through_pct: Optional[float] = None
    currency: Optional[str] = "EUR"; status: Literal["on sale","sold out","canceled","postponed"] = "on sale"
    source_url: Optional[str] = None; scrape_ts_utc: datetime; ingestion_run_id: str
class NormalizedEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")
    provider: Literal["shotgun"]; event_id_provider: str; event_name: str
    venue_name: Optional[str] = None; city: Optional[str] = None; country: Optional[str] = None
    event_datetime_local: Optional[datetime] = None; timezone: Optional[str] = "Europe/Paris"
    status: Optional[str] = "on sale"; inventory_total: Optional[int] = None; inventory_available: Optional[int] = None
    tickets_sold_total: Optional[int] = None; tickets_sold_today: Optional[int] = None; checkins_total: Optional[int] = None
    gross_total: Optional[float] = None; net_total: Optional[float] = None; gross_today: Optional[float] = None
    currency: Optional[str] = "EUR"; sell_through_pct: Optional[float] = None
    scrape_ts_utc: datetime; ingestion_run_id: str
