from dagster import Definitions, load_assets_from_modules

from .assets import summary
from .resources import database_resource

summary_assets = load_assets_from_modules([summary])

defs = Definitions(
    assets=[*summary_assets],
    resources={
        "database": database_resource,
    },
)
