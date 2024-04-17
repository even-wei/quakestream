from dagster import Definitions, load_assets_from_modules

from .assets import earthquakes
from .resources import database_resource

earthquakes_assets = load_assets_from_modules([earthquakes])

defs = Definitions(
    assets=[*earthquakes_assets],
    resources={
        "database": database_resource,
    },
)
