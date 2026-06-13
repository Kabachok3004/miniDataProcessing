"""Шаблон Dagster-ассетов. Тонкие обёртки: I/O + lineage, без бизнес-логики.

После копирования переименуй mysource/mydataset и зарегистрируй модуль
в datalake/definitions.py (ASSET_MODULES) и расписание в datalake/jobs/.
"""

from __future__ import annotations

from dagster import asset

from datalake.connectors.duckdb_s3 import get_duckdb
from datalake.connectors.s3_upload import upload_bytes_to_s3
from datalake.paths import BUCKET, raw_key, s3_uri

from .extract import fetch
from .transform import DATASET, build

SOURCE = "mysource"  # TODO: имя источника (попадёт в raw/<SOURCE>/...)


@asset(group_name="mysource")
def mysource_raw() -> str:
    key = raw_key(SOURCE, DATASET)
    upload_bytes_to_s3(fetch(), BUCKET, key)
    return key


@asset(group_name="mysource")
def mysource_processed(mysource_raw: str) -> str:
    return build(get_duckdb(), s3_uri(mysource_raw))
