"""Расписание для cbr-пайплайна: ежедневный прогон raw -> processed в 09:00."""

from dagster import ScheduleDefinition, define_asset_job

cbr_job = define_asset_job(
    name="cbr_job",
    selection=["cbr_raw", "cbr_processed"],
)

cbr_schedule = ScheduleDefinition(
    job=cbr_job,
    cron_schedule="0 9 * * *",
)
