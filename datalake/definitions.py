"""Сборка всех пайплайнов для Dagster.

Подключить пайплайн:
  - ассеты: модуль через load_assets_from_modules ИЛИ готовый список -> в assets;
  - job/schedule -> в jobs / schedules.
"""

from dagster import Definitions, load_assets_from_modules

from datalake.jobs.cbr import cbr_job, cbr_schedule
from datalake.jobs.hh import hh_job, hh_schedule
from datalake.pipelines.cbr import assets as cbr_assets
from datalake.pipelines.hh.assets import HH_ASSETS

defs = Definitions(
    assets=load_assets_from_modules([cbr_assets]) + HH_ASSETS,
    jobs=[cbr_job, hh_job],
    schedules=[cbr_schedule, hh_schedule],
)
