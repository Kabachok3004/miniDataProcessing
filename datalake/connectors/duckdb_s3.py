"""DuckDB-коннектор, настроенный на чтение/запись parquet прямо в S3.

Аналитик пишет SQL-трансформации поверх `s3://...` URI (см. paths.s3_uri),
а креды и endpoint подхватываются из тех же env, что и boto3 (.env).

    from datalake.connectors.duckdb_s3 import get_duckdb
    con = get_duckdb()
    con.execute("SELECT count(*) FROM read_parquet('s3://bucket/key')")
"""

from __future__ import annotations

import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


def get_duckdb() -> "duckdb.DuckDBPyConnection":
    """Возвращает in-memory DuckDB с загруженным httpfs и настроенным S3-секретом."""
    endpoint = os.getenv("datalake_address", "https://storage.yandexcloud.net")
    use_ssl = endpoint.startswith("https")
    host = endpoint.split("://", 1)[-1].rstrip("/")  # DuckDB ждёт endpoint без схемы

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        """
        CREATE OR REPLACE SECRET s3_yandex (
            TYPE S3,
            KEY_ID ?,
            SECRET ?,
            ENDPOINT ?,
            REGION 'ru-central1',
            URL_STYLE 'path',
            USE_SSL ?
        )
        """,
        [
            os.getenv("datalake_access_key"),
            os.getenv("datalake_secret_key"),
            host,
            use_ssl,
        ],
    )
    return con
