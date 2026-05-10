from dagster import Definitions, define_asset_job, ScheduleDefinition
from datalake.assets.cbr import cbr_raw, cbr_clean, cbr_to_s3

cbr_job = define_asset_job(
    name="cbr_job",
    selection=["cbr_raw", "cbr_clean", "cbr_to_s3"],
)

# Каждый день в 9:00 — пока можно закомментировать
cbr_schedule = ScheduleDefinition(
    job=cbr_job,
    cron_schedule="0 9 * * *",
)

defs = Definitions(
    assets=[cbr_raw, cbr_clean, cbr_to_s3],
    jobs=[cbr_job],
    schedules=[cbr_schedule],
)