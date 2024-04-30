from dagster import Definitions, load_assets_from_modules

from .assets import earthquakes, metrics, notebooks
from .resources import database_resource

earthquakes_assets = load_assets_from_modules([earthquakes])
metrics_assets = load_assets_from_modules([metrics])
notebooks_assets = load_assets_from_modules([notebooks])

defs = Definitions(
    assets=[
        *earthquakes_assets,
        *metrics_assets,
        *notebooks_assets
    ],
    resources={
        "database": database_resource,
    },
)
