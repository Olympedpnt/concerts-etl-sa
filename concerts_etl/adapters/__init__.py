from concerts_etl.adapters import shotgun
from concerts_etl.adapters import dice

REGISTRY = {
    "shotgun": shotgun.run,
    "dice": dice.run,
}
