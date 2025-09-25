from __future__ import annotations
from typing import Callable, Awaitable, Dict, List
from concerts_etl.core.models import NormalizedEvent

# Import des runners de chaque provider
from .dice import run as run_dice
from .shotgun import run as run_shotgun

# Registre des providers disponibles
REGISTRY: Dict[str, Callable[[], Awaitable[List[NormalizedEvent]]]] = {
    "shotgun": run_shotgun,
    "dice": run_dice,
}
