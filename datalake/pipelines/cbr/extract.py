"""
    python -m datalake.pipelines.cbr.extract   # дёрнуть API и глянуть размер
"""

from __future__ import annotations

from datalake.connectors.rest_api import get_bytes

CBR_URL = "https://www.cbr-xml-daily.ru/daily_json.js"


def fetch_rates() -> bytes:
    return get_bytes(CBR_URL)


if __name__ == "__main__":
    raw = fetch_rates()
    print(f"fetched {len(raw)} bytes from {CBR_URL}")
