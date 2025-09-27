# concerts_etl/adapters/__init__.py

from .shotgun import run as run_shotgun   # async def run() -> List[NormalizedEvent]
from .dice import run as run_dice         # async def run() -> List[NormalizedEvent]

REGISTRY = {
    "shotgun": run_shotgun,
    "dice": run_dice,
}
