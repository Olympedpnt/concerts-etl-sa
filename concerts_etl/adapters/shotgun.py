# (version sélecteurs basée sur ton HTML — voir message précédent)
# Pour concision ici, je te l'ai déjà fourni ; colle la même version complète.
from __future__ import annotations
import asyncio, re, uuid, logging, hashlib, unicodedata
from datetime import datetime, timezone
from typing import List
from tenacity import retry, wait_exponential, stop_after_attempt
from playwright.async_api import async_playwright
from concerts_etl.core.models import RawShotgunCard, NormalizedEvent
from concerts_etl.core.config import settings
# ... (colle ici la version complète fournie plus haut)
async def run() -> list[NormalizedEvent]:
    from concerts_etl.adapters.shotgun import _collect_cards, normalize  # placeholder si split
    cards = await _collect_cards()
    return normalize(cards)
