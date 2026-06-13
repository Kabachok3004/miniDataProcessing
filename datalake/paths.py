"""Единый источник истины для раскладки данных в S3 (медальон).

raw/<source>/<dataset>/dt=YYYY-MM-DD/...   — как пришло из источника
processed/<dataset>/dt=YYYY-MM-DD/...      — после трансформации (parquet)

"""

from __future__ import annotations

from datetime import date

BUCKET = "mini-data-lake"


def _dt(dt: date | None) -> str:
    return (dt or date.today()).isoformat()


def raw_key(source: str, dataset: str, dt: date | None = None, ext: str = "json") -> str:
    """Ключ для сырого слоя: raw/<source>/<dataset>/dt=YYYY-MM-DD/<dataset>.<ext>"""
    return f"raw/{source}/{dataset}/dt={_dt(dt)}/{dataset}.{ext}"


def processed_key(dataset: str, dt: date | None = None, ext: str = "parquet") -> str:
    """Ключ для обработанного слоя: processed/<dataset>/dt=YYYY-MM-DD/<dataset>.<ext>"""
    return f"processed/{dataset}/dt={_dt(dt)}/{dataset}.{ext}"


def s3_uri(key: str) -> str:
    """s3://<bucket>/<key> — форма, которую понимает DuckDB (httpfs)."""
    return f"s3://{BUCKET}/{key}"
