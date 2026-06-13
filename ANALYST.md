# Гайд аналитика — как добавить ETL-пайплайн

Цель: писать пайплайны (источник → сырой слой → обработанный слой → дашборд),
не погружаясь в инфраструктуру (S3-креды, Kafka, Docker, деплой — скрыты).

## Модель

Три слоя, каждый — отдельное место в репозитории:

| Слой | Где | Что это |
|---|---|---|
| **Коннекторы** | `datalake/connectors/` | общий I/O: S3, DuckDB-поверх-S3, REST API, Kafka. Готовое, трогать не нужно. |
| **Скрипты** | `datalake/pipelines/<source>/` | твоя логика: `extract.py` + `transform.py`. Чистые функции — тестируются локально. |
| **Джобы** | `datalake/jobs/` | расписание (cron) для Dagster. |

Данные в S3 (бакет `mini-data-lake`) лежат по медальону — пути строятся **только** через `datalake/paths.py`:

```
raw/<source>/<dataset>/dt=YYYY-MM-DD/...     ← как пришло из источника
processed/<dataset>/dt=YYYY-MM-DD/...         ← после трансформации (parquet)
```

Эталонный пример — `datalake/pipelines/cbr/` (курсы ЦБ).

---

## Добавить пайплайн за 4 шага

### 1. Скопировать шаблон
```sh
cp -r datalake/pipelines/_template datalake/pipelines/<source>
```
Внутри три файла: `extract.py`, `transform.py`, `assets.py`.

### 2. Написать extract (`extract.py`)
Чистая функция, возвращает сырьё как есть:
```python
from datalake.connectors.rest_api import get_bytes, get_json, paginate

SOURCE_URL = "https://api.example.com/data"

def fetch() -> bytes:
    return get_bytes(SOURCE_URL)          # есть get_json и paginate для постраничных API
```
Проверить локально:
```sh
python -m datalake.pipelines.<source>.extract
```

### 3. Написать transform (`transform.py`)
SQL поверх DuckDB. `select_sql()` держим отдельно от записи — чтобы тестировать:
```python
def select_sql(src_uri: str) -> str:
    return f"SELECT col1, CAST(col2 AS DOUBLE) AS metric FROM read_json_auto('{src_uri}')"
```
DuckDB читает прямо из `s3://`, `http://` или локального файла: `read_json_auto` / `read_csv_auto` / `read_parquet`.

### 4. Подключить ассеты и расписание
- В `assets.py` переименуй `mysource`/`mydataset` под себя (имена ассетов → имена в Dagster UI).
- В `datalake/jobs/<source>.py` заведи `define_asset_job` + `ScheduleDefinition` (cron) — см. `jobs/cbr.py`.
- В `datalake/definitions.py` добавь свой `assets`-модуль в `ASSET_MODULES`, job в `JOBS`, schedule в `SCHEDULES`.

---

## Тестирование локально

Перед коммитом — без сервера, без Dagster:

```sh
# чистая логика трансформации на фикстуре (образец — tests/test_cbr_transform.py)
PYTHONPATH=. pytest datalake/tests/ -q

# полный round-trip в S3 (extract -> raw -> transform -> processed -> чтение)
PYTHONPATH=. python -m datalake.pipelines.<source>.extract
```

> Локально нужны зависимости: `pip install duckdb httpx polars boto3 python-dotenv`
> и `.env` в корне репо (креды S3). DuckDB/SQL-логику можно гонять вообще без сети.

---

## Деплой и запуск

```sh
git push origin main        # GitHub Actions пересоберёт Dagster на VPS1
```

- **Dagster UI:** `http://91.218.115.207:3000` — там видно граф ассетов (raw → processed),
  ручные прогоны (Materialize) и расписания.
- Расписание из `ScheduleDefinition` запускается автоматически (cron в UTC).

---

## Дашборды

- Обработанные данные (`processed/<dataset>/...parquet`) читаются тем же `duckdb_s3`
  или boto3 — это вход для дашборда.
- Текущий пример дашборда (streaming-метрики): `datalake/streaming/examples/dashboard.py` (Streamlit).
- DataLens — позже, подключается к тем же S3/parquet.

---

## Шпаргалка по коннекторам

| Импорт | Что даёт |
|---|---|
| `from datalake.paths import raw_key, processed_key, s3_uri, BUCKET` | пути в S3 |
| `from datalake.connectors.rest_api import get_bytes, get_json, paginate` | HTTP-источники с ретраями |
| `from datalake.connectors.duckdb_s3 import get_duckdb` | DuckDB с доступом к S3 (httpfs) |
| `from datalake.connectors.s3_upload import upload_bytes_to_s3, download_bytes, list_keys` | прямой S3 I/O |
