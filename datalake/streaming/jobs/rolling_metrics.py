"""
PyFlink job: скользящие метрики из Kafka в S3.

Читает JSON из топика `events` (формат: {event_type, user_id, amount, ts_ms}),
строит sliding window 1 минута со сдвигом 10 секунд по event_type,
считает count + sum(amount) + avg(amount), пишет JSON в S3.

Колонка называется `amount`, а не `value` — VALUE зарезервированное слово в Flink SQL.

Запуск внутри Flink JM-контейнера (см. flink_submit.py):
    flink run -py /opt/jobs/rolling_metrics.py

Параметры берутся из env переменных, проброшенных в контейнер через FLINK_PROPERTIES:
  KAFKA_BOOTSTRAP_INTERNAL  - адрес Kafka внутри docker-сети (default: kafka:9092)
  S3_BUCKET                 - бакет для sink (default: mini-data-lake)
"""
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092")
S3_BUCKET = os.getenv("S3_BUCKET", "mini-data-lake")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "events")


def main() -> None:
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", "1")

    t_env.execute_sql(f"""
        CREATE TABLE events_src (
            event_type STRING,
            user_id    BIGINT,
            amount     DOUBLE,
            ts_ms      BIGINT,
            event_ts   AS TO_TIMESTAMP_LTZ(ts_ms, 3),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-rolling-metrics',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE metrics_sink (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            event_type   STRING,
            cnt          BIGINT,
            total        DOUBLE,
            avg_value    DOUBLE
        ) PARTITIONED BY (event_type) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://{S3_BUCKET}/streaming/rolling_metrics/',
            'format' = 'json',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '30 s'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO metrics_sink
        SELECT
            window_start,
            window_end,
            event_type,
            COUNT(*)    AS cnt,
            SUM(amount) AS total,
            AVG(amount) AS avg_value
        FROM TABLE(
            HOP(TABLE events_src, DESCRIPTOR(event_ts), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, event_type
    """).wait()


if __name__ == "__main__":
    main()
