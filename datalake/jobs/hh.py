"""Одна джоба на весь HH: справочники + вакансии параллельно, лимит 6 потоков.

Все ассеты группы "hh" материализуются в одном ране; multiprocess-executor
с max_concurrent=6 ограничивает число одновременно работающих тасок.
"""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    multiprocess_executor,
)

hh_job = define_asset_job(
    name="hh_job",
    selection=AssetSelection.groups("hh"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 6}),
)

hh_schedule = ScheduleDefinition(
    job=hh_job,
    cron_schedule="0 6 * * *",
)
