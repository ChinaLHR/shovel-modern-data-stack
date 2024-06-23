from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import imdb_dbt_assets,imdb_airbyte_assets
from .project import dbt_data_center_project
from .schedules import schedules_every_half_hour

defs = Definitions(
    assets=[imdb_airbyte_assets,imdb_dbt_assets],
    schedules=schedules_every_half_hour,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_data_center_project),
    },
)