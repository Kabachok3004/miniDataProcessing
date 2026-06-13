"""CBR transform: raw JSON -> processed parquet средствами DuckDB SQL.

Чистая логика поверх переданного DuckDB-соединения — не зависит от Dagster.
SELECT вынесен отдельно от записи, чтобы трансформацию можно было прогнать
на локальной фикстуре в тестах (без S3) — см. datalake/tests/test_cbr_transform.py.
"""

from __future__ import annotations

from datetime import date

from datalake.paths import processed_key, s3_uri

# Разворачиваем объект Valute (ключ = код валюты) в строки.
# Valute приводим к JSON, чтобы json_each() мог итерировать динамические ключи.
def select_sql(src_uri: str) -> str:
    """SELECT, превращающий сырой JSON курсов ЦБ в плоскую таблицу. Работает с любым
    источником, который понимает read_json_auto (s3://, http://, локальный файл)."""
    return f"""
        SELECT
            t.Date::date                                   AS date,
            v.key                                          AS currency,
            json_extract_string(v.value, '$.Name')         AS name,
            CAST(json_extract_string(v.value, '$.Value')   AS DOUBLE)  AS rate,
            CAST(json_extract_string(v.value, '$.Nominal') AS INTEGER) AS nominal
        FROM (
            SELECT Date, CAST(Valute AS JSON) AS valute
            FROM read_json_auto('{src_uri}')
        ) t,
        json_each(t.valute) v
        ORDER BY currency
    """


def build_rates(con, raw_uri: str, dt: date | None = None) -> str:
    """Трансформирует сырой курс ЦБ в processed parquet в S3. Возвращает processed-ключ."""
    out_key = processed_key("cbr", dt)
    con.execute(
        f"COPY ({select_sql(raw_uri)}) "
        f"TO '{s3_uri(out_key)}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    )
    return out_key
