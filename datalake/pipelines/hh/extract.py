"""HH extract: справочники (публичные) и вакансии (по app-токену).

Чистые функции — возвращают сырой JSON как есть. Тестируются локально:
    python -m datalake.pipelines.hh.extract            # справочники (без токена)
"""

from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path

import httpx

from datalake.connectors.rest_api import get_bytes

HH = "https://api.hh.ru"

# HH требует осмысленный User-Agent (иначе 403). Подставь свой контакт.
HEADERS = {"User-Agent": "mini-data-lake-analytics/1.0 (varenick.with.cherry@gmail.com)"}


# ── Справочники (публичные, без токена) ─────────────────────────────────────

def _get(path: str) -> bytes:
    return get_bytes(f"{HH}{path}", headers=HEADERS)


def fetch_professional_roles() -> bytes:
    return _get("/professional_roles")


def fetch_industries() -> bytes:
    return _get("/industries")


def fetch_languages() -> bytes:
    return _get("/languages")


def fetch_areas() -> bytes:
    return _get("/areas")


def fetch_dictionaries() -> bytes:
    return _get("/dictionaries")


DICTIONARIES = {
    "professional_roles": fetch_professional_roles,
    "industries": fetch_industries,
    "languages": fetch_languages,
    "areas": fetch_areas,
    "dictionaries": fetch_dictionaries,
}


# ── Вакансии (нужен app-токен: dev.hh.ru -> приложение -> client_credentials) ─

# App-токен длинноживущий — кешируем в файл (общий для всех воркер-процессов
# контейнера), чтобы 194 таски не минтили по токену каждая (HH банит burst на /token).
_TOKEN_CACHE = Path(os.getenv("HH_TOKEN_CACHE", "/tmp/hh_app_token.json"))


def _read_cached_token() -> str | None:
    try:
        c = json.loads(_TOKEN_CACHE.read_text())
        if c.get("expires_at", 0) - time.time() > 300:  # ещё валиден (запас 5 мин)
            return c["access_token"]
    except Exception:
        return None
    return None


def get_app_token(retries: int = 6, backoff: float = 1.5) -> str:
    """Application access token (client_credentials), с кешем и backoff.

    Возвращает закешированный токен, если он ещё валиден. Иначе запрашивает новый,
    переживая 403/429 на /token (burst-троттлинг) и гонку между воркерами.
    """
    cached = _read_cached_token()
    if cached:
        return cached

    cid, sec = os.getenv("HH_CLIENT_ID"), os.getenv("HH_CLIENT_SECRET")
    if not cid or not sec:
        raise RuntimeError(
            "HH_CLIENT_ID / HH_CLIENT_SECRET не заданы в .env "
            "(зарегистрируй приложение на https://dev.hh.ru)"
        )

    last: httpx.Response | None = None
    for attempt in range(retries):
        cached = _read_cached_token()  # другой воркер мог уже получить токен
        if cached:
            return cached
        r = httpx.post(
            f"{HH}/token",
            data={"grant_type": "client_credentials", "client_id": cid, "client_secret": sec},
            headers=HEADERS,
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            token = data["access_token"]
            expires_at = time.time() + int(data.get("expires_in", 14 * 24 * 3600))
            try:  # атомарная запись кеша
                tmp = _TOKEN_CACHE.with_suffix(".tmp")
                tmp.write_text(json.dumps({"access_token": token, "expires_at": expires_at}))
                tmp.replace(_TOKEN_CACHE)
            except Exception:
                pass
            return token
        if r.status_code in (403, 429, 503):  # burst-троттлинг — ждём и пробуем снова
            ra = r.headers.get("Retry-After")
            wait = float(ra) if (ra and ra.isdigit()) else backoff * (2 ** attempt)
            time.sleep(min(wait, 60) + random.uniform(0, 1))  # jitter против гонки
            last = r
            continue
        r.raise_for_status()
    if last is not None:
        last.raise_for_status()
    raise RuntimeError("get_app_token: ретраи исчерпаны")


def _get_with_backoff(
    client: httpx.Client,
    path: str,
    params: dict,
    *,
    retries: int = 5,
    backoff: float = 1.0,
    max_wait: float = 60.0,
) -> httpx.Response:
    """GET с ретраями на 429/503 (HH троттлит при fan-out на сотни ролей).

    Пауза = Retry-After из ответа, иначе экспоненциальный backoff.
    """
    r = None
    for attempt in range(retries):
        r = client.get(path, params=params)
        if r.status_code not in (429, 503):
            r.raise_for_status()
            return r
        retry_after = r.headers.get("Retry-After")
        wait = float(retry_after) if (retry_after and retry_after.isdigit()) else backoff * (2 ** attempt)
        time.sleep(min(wait, max_wait))
    # ретраи исчерпаны — пусть последний ответ бросит ошибку
    r.raise_for_status()
    return r


def fetch_vacancies(
    token: str,
    *,
    professional_role: str | None = None,
    area: str | None = None,
    text: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    period: int = 30,
    per_page: int = 100,
    max_pages: int = 5,
) -> bytes:
    """Вакансии по фильтру, склеенные по страницам -> JSON-массив items (bytes).

    date_from/date_to (ISO-дата публикации) задают инкрементальное окно; их
    нельзя слать вместе с period, поэтому при заданном date_from period опускаем.
    """
    headers = {**HEADERS, "Authorization": f"Bearer {token}"}
    params: dict = {"per_page": per_page}
    if date_from is not None:
        params["date_from"] = date_from
        if date_to is not None:
            params["date_to"] = date_to
    else:
        params["period"] = period
    if professional_role is not None:
        params["professional_role"] = professional_role
    if area is not None:
        params["area"] = area
    if text:
        params["text"] = text

    items: list = []
    with httpx.Client(base_url=HH, headers=headers, timeout=30) as c:
        for page in range(max_pages):
            params["page"] = page
            r = _get_with_backoff(c, "/vacancies", params)
            data = r.json()
            items.extend(data.get("items", []))
            if page >= data.get("pages", 1) - 1:
                break
    return json.dumps(items, ensure_ascii=False).encode()


# Ключи fan-out для вакансий — все профессиональные роли HH (кроме deprecated,
# по которым поиск вакансий отключён). Полный справочник — в roles.py.
from datalake.pipelines.hh.roles import PROFESSIONAL_ROLES, SEARCH_DEPRECATED

VACANCY_ROLES = {
    rid: name for rid, name in PROFESSIONAL_ROLES.items() if rid not in SEARCH_DEPRECATED
}


if __name__ == "__main__":
    for name, fn in DICTIONARIES.items():
        print(f"{name}: {len(fn())} bytes")
