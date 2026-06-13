"""Юнит-тест трансформации cbr на локальной фикстуре — без S3 и сети.

Проверяем именно SQL-логику (разворот Valute, типы, нормализация),
поэтому читаем локальный JSON-файл, а не s3://.
"""

import json

import duckdb

from datalake.pipelines.cbr.transform import select_sql

FIXTURE = {
    "Date": "2026-06-12T11:30:00+03:00",
    "Valute": {
        "USD": {"Name": "Доллар США", "Value": 71.9077, "Nominal": 1},
        "EUR": {"Name": "Евро", "Value": 82.9743, "Nominal": 1},
        "JPY": {"Name": "Иена", "Value": 49.5, "Nominal": 100},
    },
}


def test_select_unnests_and_types(tmp_path):
    src = tmp_path / "rates.json"
    src.write_text(json.dumps(FIXTURE), encoding="utf-8")

    con = duckdb.connect()
    rows = con.execute(select_sql(str(src))).fetchall()
    cols = [c[0] for c in con.description]

    assert cols == ["date", "currency", "name", "rate", "nominal"]
    assert len(rows) == 3  # три валюты развернулись в три строки

    by_cur = {r[1]: r for r in rows}
    assert by_cur["USD"][3] == 71.9077          # rate -> DOUBLE
    assert by_cur["JPY"][4] == 100              # nominal -> INTEGER
    assert by_cur["EUR"][2] == "Евро"           # name
    assert str(by_cur["USD"][0]) == "2026-06-12"  # date нормализован
