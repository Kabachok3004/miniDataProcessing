import io
from datetime import datetime, timezone
import polars as pl
import httpx
from dagster import asset, AssetExecutionContext

from connectors.s3_upload import upload_bytes_to_s3

BUCKET = "mini-data-lake"


@asset(group_name="cbr")
def cbr_raw(context: AssetExecutionContext) -> dict:
    response = httpx.get("https://www.cbr-xml-daily.ru/daily_json.js", timeout=10)
    data = response.json()
    context.log.info(f"Получены данные за {data['Date'][:10]}, валют: {len(data['Valute'])}")
    return data


@asset(group_name="cbr")
def cbr_clean(context: AssetExecutionContext, cbr_raw: dict) -> pl.DataFrame:
    rows = [
        {
            "date": cbr_raw["Date"][:10],
            "currency": code,
            "rate": info["Value"],
            "name": info["Name"],
        }
        for code, info in cbr_raw["Valute"].items()
    ]

    df = (
        pl.DataFrame(rows)
        .with_columns(pl.col("date").str.to_date())
        .with_columns(pl.col("rate").cast(pl.Float64).round(4))
        .sort("currency")
    )
    context.log.info(f"Чистых строк: {len(df)}")
    return df


@asset(group_name="cbr")
def cbr_to_s3(context: AssetExecutionContext, cbr_clean: pl.DataFrame) -> str:
    """Сохраняет курсы валют в S3 как Parquet с timestamp в имени."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"data/cbr/cb_exchange_{ts}.parquet"

    # DataFrame → bytes прямо в память, без временного файла
    buf = io.BytesIO()
    cbr_clean.write_parquet(buf)

    s3_path = upload_bytes_to_s3(
        data=buf.getvalue(),
        bucket=BUCKET,
        key=key,
    )

    context.log.info(f"Загружено: {s3_path}")
    return s3_path