"""Шаблон transform. raw -> processed parquet средствами DuckDB SQL.

select_sql() держим отдельно от записи, чтобы тестировать на локальной фикстуре
(см. datalake/tests/test_cbr_transform.py как образец).
"""

from __future__ import annotations

from datetime import date

from datalake.paths import processed_key, s3_uri

DATASET = "mydataset"  # TODO: имя датасета (попадёт в processed/<DATASET>/...)


def select_sql(src_uri: str) -> str:
    """SQL-трансформация сырья. src_uri понимает s3://, http:// и локальный файл."""
    # TODO: заменить на реальную логику (read_json_auto / read_csv_auto / read_parquet)
    return f"SELECT * FROM read_json_auto('{src_uri}')"


def build(con, raw_uri: str, dt: date | None = None) -> str:
    out_key = processed_key(DATASET, dt)
    con.execute(
        f"COPY ({select_sql(raw_uri)}) "
        f"TO '{s3_uri(out_key)}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    )
    return out_key
