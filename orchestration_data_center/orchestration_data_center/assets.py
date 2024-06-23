import os
from dagster import AssetExecutionContext, EnvVar
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_airbyte import AirbyteResource, load_assets_from_airbyte_instance
from .project import dbt_data_center_project


airbyte_instance = AirbyteResource(
    host="127.0.0.1",
    port="8000",
    # If using basic auth, include username and password:
    # username="airbyte",
    # password=os.getenv("AIRBYTE_PASSWORD")
)

imdb_airbyte_assets = load_assets_from_airbyte_instance(
    airbyte_instance,
    connection_filter=lambda meta: "imdb_to_data_center" in meta.name,)

@dbt_assets(manifest=dbt_data_center_project.manifest_path,select='tag:dbt_imdb')
def imdb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()