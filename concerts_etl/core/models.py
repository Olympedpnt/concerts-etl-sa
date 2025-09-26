# concerts_etl/core/models.py

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class RawShotgunCard(BaseModel):
    """
    Modèle 'brut' pour les cartes Shotgun telles que récupérées par le scraper.
    Sert d'étape intermédiaire avant normalisation.
    """
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

    # Tolérant aux champs en plus (si le scraper ajoute des clés temporaires)
    model_config = ConfigDict(extra="allow")


class NormalizedEvent(BaseModel):
    """
    Modèle commun à tous les providers (Shotgun, Dice, etc.).
    C'est sur cette base que la consolidation est effectuée.
    """
    provider: str                       # ex: "shotgun" | "dice"
    event_id_provider: str              # id côté provider
    event_name: str

    # --- NOUVEAUX CHAMPS POUR LE MATCHING ---
    artist_name: Optional[str] = None   # nom d’artiste principal si dispo
    venue_name: Optional[str] = None    # nom de salle/lieu si dispo

    city: Optional[str]
    country: Optional[str]
    event_datetime_local: Optional[datetime]
    timezone: str                       # ex: "Europe/Paris"
    status: str                         # ex: "on sale", "sold out", etc.
    tickets_sold_total: Optional[int]
    gross_total: Optional[float]
    net_total: Optional[float]
    currency: Optional[str]
    sell_through_pct: Optional[float]
    scrape_ts_utc: datetime
    ingestion_run_id: str

    # Tolérant aux champs en plus (si on souhaite enrichir ponctuellement)
    model_config = ConfigDict(extra="allow")
