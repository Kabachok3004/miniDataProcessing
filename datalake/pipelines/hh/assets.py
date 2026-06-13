"""HH Dagster-ассеты: справочники + вакансии (fan-out по ролям).

Тонкие обёртки над extract/transform. Все в группе "hh" — джоба hh_job
материализует их параллельно (см. datalake/jobs/hh.py).
"""

from __future__ import annotations

from datetime import date

from dagster import asset

from datalake.connectors.duckdb_s3 import get_duckdb
from datalake.connectors.s3_upload import upload_bytes_to_s3
from datalake.paths import BUCKET, raw_key, s3_uri

from . import extract, transform


# ── Справочники (без токена) ────────────────────────────────────────────────

def _dictionary_asset(name, fetch, build):
    @asset(name=f"hh_{name}", group_name="hh")
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


# ── Вакансии (app-токен), отдельная таска на каждую роль ────────────────────

def _vacancy_asset(role_id: str, role_name: str):
    @asset(name=f"hh_vacancies_role_{role_id}", group_name="hh", description=f"Вакансии: {role_name}")
    def _a() -> str:
        token = extract.get_app_token()
        raw = extract.fetch_vacancies(token, professional_role=role_id)
        d = date.today().isoformat()
        rkey = f"raw/hh/vacancies/dt={d}/role_id={role_id}/vacancies.json"
        upload_bytes_to_s3(raw, BUCKET, rkey)
        return transform.build_vacancies(get_duckdb(), s3_uri(rkey), role_id)

    return _a


vacancy_assets = [_vacancy_asset(rid, rname) for rid, rname in extract.VACANCY_ROLES.items()]

# всё HH (справочники + вакансии) — подключается в definitions.py
HH_ASSETS = dictionary_assets + vacancy_assets
