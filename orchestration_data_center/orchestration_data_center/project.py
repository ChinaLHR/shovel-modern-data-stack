from pathlib import Path

from dagster_dbt import DbtProject

dbt_data_center_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "dbt_data_center").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)