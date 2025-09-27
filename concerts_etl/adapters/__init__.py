# concerts_etl/adapters/__init__.py

from .shotgun import run as run_shotgun
from .dice import run as run_dice

__all__ = ["run_shotgun", "run_dice"]
