# concerts_etl/core/model.py
from __future__ import annotations

from typing import Optional
from datetime import datetime
from pydantic import BaseModel

class RawShotgunCard(BaseModel):
    event_id_provider: str
    event_name: str
    event_datetime_local: Optional[datetime] = None
    city: Optional[str] = None
    country: Optional[str] = None
    gross_total: Optional[float] = None
    gross_today: Optional[float] = None
    tickets_sold_total: Optional[int] = None
    sell_through_pct: Optional[float] = None
    currency: Optional[str] = None
    status: str
    source_url: str
    scrape_ts_utc: datetime
    ingestion_run_id: str
    # facultatif côté Shotgun brut
    artist_name: Optional[str] = None
    venue_name: Optional[str] = None

class NormalizedEvent(BaseModel):
    provider: str
    event_id_provider: str
    event_name: str
    city: Optional[str] = None
    country: Optional[str] = None
    event_datetime_local: Optional[datetime] = None
    timezone: str
    status: str
    tickets_sold_total: Optional[int] = None
    gross_total: Optional[float] = None
    net_total: Optional[float] = None
    currency: Optional[str] = None
    sell_through_pct: Optional[float] = None
    scrape_ts_utc: datetime
    ingestion_run_id: str

    # ⬇️ Ajouts pour faciliter la jointure SG↔DICE
    artist_name: Optional[str] = None
    venue_name: Optional[str] = None
