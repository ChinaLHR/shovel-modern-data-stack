"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import define_asset_job, ScheduleDefinition, Definitions

run_all_asset_job = define_asset_job("run_all_asset", selection="*")

schedules_every_half_hour = [
    ScheduleDefinition(
        job=run_all_asset_job,
        cron_schedule="*/30 * * * *",  
    )
]