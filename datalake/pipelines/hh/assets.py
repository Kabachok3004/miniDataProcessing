"""HH Dagster-ассеты: справочники (full-refresh) + вакансии (инкремент по дням).

Тонкие обёртки над extract/transform.
- Справочники — группа "hh_dictionaries", без партиций (маленькие, перегружаем целиком).
- Вакансии — группа "hh_vacancies", DailyPartitionsDefinition: ключ партиции = день,
  тянем только окно публикаций за этот день (date_from/date_to). Watermark — сам Dagster.
"""

from __future__ import annotations

from datetime import date, timedelta

from dagster import DailyPartitionsDefinition, asset

from datalake.connectors.duckdb_s3 import get_duckdb
from datalake.connectors.s3_upload import upload_bytes_to_s3
from datalake.paths import BUCKET, raw_key, s3_uri

from . import extract, transform

# С какой даты доступны партиции вакансий (бэкфилл за более ранние — из UI).
VACANCIES_START = "2026-06-01"
daily = DailyPartitionsDefinition(start_date=VACANCIES_START)


# ── Справочники (без токена, full-refresh) ──────────────────────────────────

def _dictionary_asset(name, fetch, build):
    @asset(name=f"hh_{name}", group_name="hh_dictionaries")
    def _a() -> str:
        rkey = raw_key("hh", name)
        upload_bytes_to_s3(fetch(), BUCKET, rkey)
        return build(get_duckdb(), s3_uri(rkey))

    return _a


dictionary_assets = [
    _dictionary_asset("professional_roles", extract.fetch_professional_roles, transform.build_professional_roles),
    _dictionary_asset("industries", extract.fetch_industries, transform.build_industries),
    _dictionary_asset("languages", extract.fetch_languages, transform.build_languages),
    _dictionary_asset("areas", extract.fetch_areas, transform.build_areas),
    _dictionary_asset("dictionaries", extract.fetch_dictionaries, transform.build_dictionaries),
]


# ── Вакансии (app-токен), отдельная таска на роль, инкремент по дням ─────────

def _vacancy_asset(role_id: str, role_name: str):
    @asset(
        name=f"hh_vacancies_role_{role_id}",
        group_name="hh_vacancies",
        partitions_def=daily,
        description=f"Вакансии: {role_name}",
    )
    def _a(context) -> str:
        day = context.partition_key                                  # "YYYY-MM-DD" = watermark
        nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        token = extract.get_app_token()
        raw = extract.fetch_vacancies(token, professional_role=role_id, date_from=day, date_to=nxt)
        rkey = f"raw/hh/vacancies/dt={day}/role_id={role_id}/vacancies.json"
        upload_bytes_to_s3(raw, BUCKET, rkey)
        return transform.build_vacancies(get_duckdb(), s3_uri(rkey), role_id, dt=date.fromisoformat(day))

    return _a


vacancy_assets = [_vacancy_asset(rid, rname) for rid, rname in extract.VACANCY_ROLES.items()]

HH_ASSETS = dictionary_assets + vacancy_assets
