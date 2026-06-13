"""Шаблон extract. Скопируй папку _template -> <source> и заполни TODO.

Чистая функция: тянет сырьё из источника и возвращает байты как есть.
Гоняется локально:  python -m datalake.pipelines.<source>.extract
"""

from __future__ import annotations

from datalake.connectors.rest_api import get_bytes  # есть ещё get_json / paginate

SOURCE_URL = "https://example.com/TODO"  # TODO: адрес источника


def fetch() -> bytes:
    # TODO: при необходимости — params/headers/пагинация (см. connectors/rest_api.py)
    return get_bytes(SOURCE_URL)


if __name__ == "__main__":
    print(f"fetched {len(fetch())} bytes")
