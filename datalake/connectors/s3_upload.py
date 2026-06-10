import os
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("datalake_address", "https://storage.yandexcloud.net"),
        aws_access_key_id=os.getenv("datalake_access_key"),
        aws_secret_access_key=os.getenv("datalake_secret_key"),
        region_name="ru-central1",
    )

 
def upload_to_s3(
    local_path: str | Path,
    bucket: str,
    key: str,                  # путь внутри бакета, например "cbr/file.parquet"
) -> str:
    """
    Загружает любой файл в S3-совместимое хранилище.

    Возвращает полный s3-путь: s3://bucket/key
    """
    path = Path(local_path)

    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    client = get_s3_client()
    client.upload_file(
        Filename=str(path),
        Bucket=bucket,
        Key=key,
    )

    return f"s3://{bucket}/{key}"


def upload_bytes_to_s3(
    data: bytes,
    bucket: str,
    key: str,
) -> str:

    client = get_s3_client()
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
    )

    return f"s3://{bucket}/{key}"