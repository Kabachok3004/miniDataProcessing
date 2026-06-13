"""Сборка всех пайплайнов для Dagster.

Чтобы подключить новый пайплайн:
  1) добавь его assets-модуль в ASSET_MODULES;
  2) добавь его job/schedule в JOBS / SCHEDULES.
"""

from dagster import Definitions, load_assets_from_modules

from datalake.jobs.cbr import cbr_job, cbr_schedule
from datalake.pipelines.cbr import assets as cbr_assets

# ── реестр пайплайнов ────────────────────────────────
ASSET_MODULES = [cbr_assets]
JOBS = [cbr_job]
SCHEDULES = [cbr_schedule]
# ─────────────────────────────────────────────────────

defs = Definitions(
    assets=load_assets_from_modules(ASSET_MODULES),
    jobs=JOBS,
    schedules=SCHEDULES,
)
