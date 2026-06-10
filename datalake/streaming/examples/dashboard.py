
import json
import re
import time
from datetime import datetime, timezone

import polars as pl
import streamlit as st

from datalake.connectors.s3_upload import get_s3_client

BUCKET = "mini-data-lake"
PREFIX = "data/mini-data-lake/streaming/rolling_metrics/"
LOOKBACK_HOURS = 4         
PAGE_SIZE = 100             


@st.cache_data(ttl=20, show_spinner="Читаем из S3…")
def load_metrics() -> pl.DataFrame:
    """
    Листает объекты в S3, пропускает .inprogress,
    извлекает event_type из пути и парсит NDJSON-файлы.
    """
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    records: list[dict] = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]

            # пропускаем in-progress файлы и «папки»
            if ".inprogress" in key or key.endswith("/"):
                continue

            # event_type из пути вида event_type=click/part-...
            match = re.search(r"event_type=([^/]+)", key)
            if not match:
                continue
            event_type = match.group(1)

            try:
                body = (
                    client.get_object(Bucket=BUCKET, Key=key)["Body"]
                    .read()
                    .decode("utf-8")
                )
            except client.exceptions.NoSuchKey:
                # Flink переименовал файл между list и get — просто пропускаем
                continue
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    row["event_type"] = event_type
                    records.append(row)
                except json.JSONDecodeError:
                    continue

    if not records:
        return pl.DataFrame()

    return (
        pl.DataFrame(records)
        .with_columns([
            pl.col("window_start").str.to_datetime(
                "%Y-%m-%d %H:%M:%S%.f", strict=False
            ).alias("window_start"),
            pl.col("window_end").str.to_datetime(
                "%Y-%m-%d %H:%M:%S%.f", strict=False
            ).alias("window_end"),
            pl.col("cnt").cast(pl.Int64),
            pl.col("total").cast(pl.Float64).round(2),
            pl.col("avg_value").cast(pl.Float64).round(4),
        ])
        .sort("window_end")
        .unique(subset=["window_start", "window_end", "event_type"])
    )


def filter_recent(df: pl.DataFrame, hours: int) -> pl.DataFrame:
    if df.is_empty():
        return df
    cutoff = df["window_end"].max() - pl.duration(hours=hours)
    return df.filter(pl.col("window_end") >= cutoff)



def render_kpis(df: pl.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего окон",     df.height)
    c2.metric("Типов событий",  df["event_type"].n_unique())
    total_events = int(df["cnt"].sum())
    c3.metric("Суммарно событий", f"{total_events:,}")
    last_ts = df["window_end"].max()
    c4.metric("Последнее окно", str(last_ts)[:16] if last_ts else "—")


def render_count_chart(df: pl.DataFrame) -> None:
    st.subheader("📈 Количество событий за окно (cnt)")
    pivot = (
        df.select(["window_end", "event_type", "cnt"])
        .pivot(on="event_type", index="window_end", values="cnt",
               aggregate_function="sum")
        .sort("window_end")
    )
    st.line_chart(pivot, x="window_end", height=300)


def render_avg_chart(df: pl.DataFrame) -> None:
    st.subheader("💰 Средний amount за окно (avg_value)")
    pivot = (
        df.select(["window_end", "event_type", "avg_value"])
        .pivot(on="event_type", index="window_end", values="avg_value",
               aggregate_function="mean")
        .sort("window_end")
    )
    st.line_chart(pivot, x="window_end", height=300)


def render_bar_totals(df: pl.DataFrame) -> None:
    st.subheader("🏆 Суммарный объём по типу события (total)")
    summary = (
        df.group_by("event_type")
        .agg(pl.col("total").sum().round(2).alias("total_sum"))
        .sort("total_sum", descending=True)
    )
    st.bar_chart(summary, x="event_type", y="total_sum", height=250)


def render_table(df: pl.DataFrame) -> None:
    st.subheader(f"📋 Последние {PAGE_SIZE} окон")
    display = (
        df.select(["window_start", "window_end", "event_type", "cnt", "total", "avg_value"])
        .tail(PAGE_SIZE)
    )
    st.dataframe(display, use_container_width=True, hide_index=True)



def main() -> None:
    st.set_page_config(
        page_title="Mini Pipeline · Live Metrics",
        page_icon="📊",
        layout="wide",
    )

    # ── sidebar ──────────────────────────────
    with st.sidebar:
        st.title("⚙️ Настройки")
        auto = st.checkbox("Автообновление", value=True)
        interval = st.slider("Интервал (сек)", 10, 120, 30) if auto else 30
        lookback = st.slider("Глубина (часов)", 1, 24, LOOKBACK_HOURS)

        if st.button("🔄 Обновить сейчас"):
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption(
            f"Обновлено: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )
        st.caption(f"Источник: s3://{BUCKET}/{PREFIX}")

    # ── header ───────────────────────────────
    st.title("📊 Rolling Metrics Dashboard")
    st.caption(
        "Flink HOP window 1 min / slide 10 s · "
        f"показываем последние {lookback} ч"
    )

    # ── data ─────────────────────────────────
    df_all = load_metrics()

    if df_all.is_empty():
        st.info(
            "Нет данных в S3. Запусти продюсер и Flink-джоб:\n\n"
            "```\n"
            "python -m datalake.streaming.examples.produce_test_events --rate 10 --loop\n"
            "python -m datalake.streaming.flink_submit rolling_metrics.py --detach\n"
            "```"
        )
        if auto:
            time.sleep(interval)
            st.rerun()
        return

    df = filter_recent(df_all, lookback)

    # ── KPIs ─────────────────────────────────
    render_kpis(df)
    st.divider()

    # ── tabs ─────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Count", "💰 Avg amount", "🏆 Totals", "📋 Таблица"
    ])
    with tab1:
        render_count_chart(df)
    with tab2:
        render_avg_chart(df)
    with tab3:
        render_bar_totals(df)
    with tab4:
        render_table(df)

    # ── auto-refresh ─────────────────────────
    if auto:
        with st.spinner(f"Следующее обновление через {interval} сек…"):
            time.sleep(interval)
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
