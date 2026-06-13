"""HH transform: раскладка справочников в реляционный вид (SELECT всех полей).

На этом этапе — только flatten вложенного JSON в плоские таблицы, без бизнес-логики.
Агрегации/витрины строятся отдельными пайплайнами поверх processed/-таблиц.

select_*_sql() вынесены отдельно от записи, чтобы прогонять на локальной фикстуре
(read_json_auto/read_text одинаково читают локальный файл, s3:// и http://).
"""

from __future__ import annotations

from datetime import date

from datalake.paths import processed_key, s3_uri


# ── SELECT-ы (flatten). src_uri: локальный путь, s3:// или http:// ──────────────

def professional_roles_sql(src_uri: str) -> str:
    """categories[] -> roles[] : одна строка = одна роль."""
    return f"""
        SELECT
            cat.id   AS category_id,   cat.name  AS category_name,
            role.id  AS role_id,       role.name AS role_name,
            role.accept_incomplete_resumes AS accept_incomplete_resumes,
            role.is_default                AS is_default,
            role.select_deprecated         AS select_deprecated,
            role.search_deprecated         AS search_deprecated
        FROM read_json_auto('{src_uri}') t,
             UNNEST(t.categories) AS c(cat),
             UNNEST(cat.roles)    AS r(role)
        ORDER BY category_id::INT, role_id::INT
    """


def industries_sql(src_uri: str) -> str:
    """группа[] -> industries[] : одна строка = одна отрасль."""
    return f"""
        SELECT
            g.id  AS industry_group_id, g.name  AS industry_group_name,
            sub.id AS industry_id,      sub.name AS industry_name
        FROM read_json_auto('{src_uri}') AS g,
             UNNEST(g.industries) AS s(sub)
    """


def languages_sql(src_uri: str) -> str:
    """Плоский справочник языков."""
    return f"SELECT id, name, uid FROM read_json_auto('{src_uri}')"


def areas_sql(src_uri: str) -> str:
    """Дерево регионов -> плоская таблица (рекурсивный обход по полю areas)."""
    return f"""
        WITH RECURSIVE src AS (SELECT json(content) AS j FROM read_text('{src_uri}')),
        nodes(node) AS (
            SELECT unnest(from_json(j, '["JSON"]')) FROM src
            UNION ALL
            SELECT unnest(from_json(json_extract(node, '$.areas'), '["JSON"]'))
            FROM nodes
            WHERE json_array_length(json_extract(node, '$.areas')) > 0
        )
        SELECT
            json_extract_string(node, '$.id')        AS area_id,
            json_extract_string(node, '$.parent_id') AS parent_id,
            json_extract_string(node, '$.name')      AS name,
            TRY_CAST(json_extract_string(node, '$.lat') AS DOUBLE) AS lat,
            TRY_CAST(json_extract_string(node, '$.lng') AS DOUBLE) AS lng
        FROM nodes
    """


def dictionaries_sql(src_uri: str) -> str:
    """Бандл /dictionaries -> длинная таблица (dict_name, id, name) по всем под-справочникам."""
    return f"""
        WITH src AS (SELECT json(content) AS j FROM read_text('{src_uri}'))
        SELECT
            je.key                              AS dict_name,
            json_extract_string(item, '$.id')   AS id,
            json_extract_string(item, '$.name') AS name
        FROM src, json_each(j) AS je,
             UNNEST(from_json(je.value, '["JSON"]')) AS u(item)
        WHERE json_type(je.value) = 'ARRAY'
    """


# ── запись в processed/ ─────────────────────────────────────────────────────

def _write(con, select: str, dataset: str, dt: date | None) -> str:
    out_key = processed_key(dataset, dt)
    con.execute(
        f"COPY ({select}) TO '{s3_uri(out_key)}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    )
    return out_key


def build_professional_roles(con, raw_uri, dt=None):
    return _write(con, professional_roles_sql(raw_uri), "hh_professional_roles", dt)


def build_industries(con, raw_uri, dt=None):
    return _write(con, industries_sql(raw_uri), "hh_industries", dt)


def build_languages(con, raw_uri, dt=None):
    return _write(con, languages_sql(raw_uri), "hh_languages", dt)


def build_areas(con, raw_uri, dt=None):
    return _write(con, areas_sql(raw_uri), "hh_areas", dt)


def build_dictionaries(con, raw_uri, dt=None):
    return _write(con, dictionaries_sql(raw_uri), "hh_dictionaries", dt)


# ── Вакансии ────────────────────────────────────────────────────────────────

def vacancies_sql(src_uri: str) -> str:
    """items[] -> плоская таблица; роли вакансии сохраняем как массивы (UNNEST позже)."""
    return f"""
        SELECT
            id, name,
            area.id        AS area_id,        area.name      AS area_name,
            employer.id    AS employer_id,    employer.name  AS employer_name,
            salary."from"  AS salary_from,    salary.to      AS salary_to,
            salary.currency AS salary_currency, salary.gross  AS salary_gross,
            experience.id  AS experience_id,  experience.name  AS experience_name,
            employment.id  AS employment_id,  employment.name  AS employment_name,
            schedule.id    AS schedule_id,    schedule.name    AS schedule_name,
            type.id        AS type_id,        type.name        AS type_name,
            published_at, created_at, alternate_url,
            list_transform(professional_roles, x -> x.id)   AS professional_role_ids,
            list_transform(professional_roles, x -> x.name) AS professional_role_names
        FROM read_json_auto('{src_uri}')
    """


def build_vacancies(con, raw_uri: str, role_id: str, dt: date | None = None) -> str:
    """Вакансии по одной роли -> processed/hh_vacancies/dt=.../role_id=<id>/."""
    d = (dt or date.today()).isoformat()
    out_key = f"processed/hh_vacancies/dt={d}/role_id={role_id}/vacancies.parquet"
    con.execute(
        f"COPY ({vacancies_sql(raw_uri)}) "
        f"TO '{s3_uri(out_key)}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
    )
    return out_key
