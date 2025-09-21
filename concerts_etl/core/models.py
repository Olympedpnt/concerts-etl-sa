from pydantic import BaseModel, ConfigDict
from typing import Optional, Literal
from datetime import datetime
class RawShotgunCard(BaseModel):
    event_id_provider: str
    event_name: str
    event_datetime_local: Optional[datetime]
    city: Optional[str]
    country: Optional[str]
    gross_total: Optional[float]
    tickets_sold_total: Optional[int]
    sell_through_pct: Optional[float]
    currency: Optional[str]
    status: str
    source_url: str
    scrape_ts_utc: datetime
    ingestion_run_id: str
class NormalizedEvent(BaseModel):
    provider: str
    event_id_provider: str
    event_name: str
    city: Optional[str]
    country: Optional[str]
    event_datetime_local: Optional[datetime]
    timezone: str
    status: str
    tickets_sold_total: Optional[int]
    gross_total: Optional[float]
    net_total: Optional[float]
    currency: Optional[str]
    sell_through_pct: Optional[float]
    scrape_ts_utc: datetime
    ingestion_run_id: str

