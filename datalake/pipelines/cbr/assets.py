"""CBR Dagster-ассеты — тонкие обёртки над extract/transform.

Логики тут нет: только I/O через коннекторы + проброс lineage (raw -> processed)
в Dagster UI. Вся бизнес-логика — в extract.py / transform.py (тестируется без Dagster).
"""

from __future__ import annotations

from dagster import asset

from datalake.connectors.duckdb_s3 import get_duckdb
from datalake.connectors.s3_upload import upload_bytes_to_s3
from datalake.paths import BUCKET, raw_key, s3_uri

from .extract import fetch_rates
from .transform import build_rates


@asset(group_name="cbr")
def cbr_raw() -> str:
    """Сырой JSON курсов ЦБ -> raw-слой S3. Возвращает ключ."""
    key = raw_key("cbr", "rates")
    upload_bytes_to_s3(fetch_rates(), BUCKET, key)
    return key


@asset(group_name="cbr")
def cbr_processed(cbr_raw: str) -> str:
    """raw JSON -> processed parquet (DuckDB SQL). Возвращает ключ processed-слоя."""
    return build_rates(get_duckdb(), s3_uri(cbr_raw))
