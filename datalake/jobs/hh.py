"""Джобы HH: справочники (full) и вакансии (инкремент по дням), лимит 6 потоков.

Партиционированные (вакансии) и непартиционированные (справочники) ассеты
нельзя держать в одной джобе — поэтому две раздельные.
"""

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
    define_asset_job,
    multiprocess_executor,
)

_executor = multiprocess_executor.configured({"max_concurrent": 6})

# Справочники: целиком, ежедневно.
hh_dictionaries_job = define_asset_job(
    name="hh_dictionaries_job",
    selection=AssetSelection.groups("hh_dictionaries"),
    executor_def=_executor,
)
hh_dictionaries_schedule = ScheduleDefinition(
    job=hh_dictionaries_job,
    cron_schedule="0 5 * * *",
)

# Вакансии: партиционированы по дням -> расписание само материализует новый день.
hh_vacancies_job = define_asset_job(
    name="hh_vacancies_job",
    selection=AssetSelection.groups("hh_vacancies"),
    executor_def=_executor,
)
hh_vacancies_schedule = build_schedule_from_partitioned_job(hh_vacancies_job)
