"""Тонкая обёртка над httpx для extract-слоя: GET с ретраями и пагинацией.

Чистые функции — гоняются и тестируются локально без Dagster.

    from datalake.connectors.rest_api import get_json, get_bytes
    data = get_json("https://www.cbr-xml-daily.ru/daily_json.js")
"""

from __future__ import annotations

import json
import time
from typing import Any, Iterator

import httpx


def get_bytes(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: float = 10.0,
    retries: int = 3,
    backoff: float = 0.5,
) -> bytes:
    """GET с экспоненциальными ретраями. Возвращает сырое тело ответа."""
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            resp = httpx.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPError as err:
            last_err = err
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    raise last_err  # type: ignore[misc]


def get_json(url: str, **kwargs: Any) -> Any:
    """GET + json.loads. kwargs пробрасываются в get_bytes."""
    return json.loads(get_bytes(url, **kwargs))


def paginate(
    url: str,
    *,
    page_param: str = "page",
    start_page: int = 1,
    params: dict | None = None,
    items_key: str | None = None,
    **kwargs: Any,
) -> Iterator[Any]:
    """Итерирует страницы, пока ответ непустой.

    items_key — ключ со списком в JSON-ответе (если None, ждём список на верхнем уровне).
    Останавливается, когда страница вернула пустой список.
    """
    page = start_page
    while True:
        merged = {**(params or {}), page_param: page}
        payload = get_json(url, params=merged, **kwargs)
        items = payload[items_key] if items_key else payload
        if not items:
            return
        yield from items
        page += 1
