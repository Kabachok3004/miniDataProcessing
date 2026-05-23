"""
Простой Python-интерфейс к Kafka (KRaft single-node на VPS).

Использование (локально):
    export KAFKA_BOOTSTRAP=<vps_ip>:9094
    from datalake.connectors.kafka_client import produce_json, consume_json, ensure_topic

    ensure_topic("events")
    produce_json("events", {"user_id": 1, "value": 42})

    for record in consume_json("events", group_id="dev"):
        print(record)
"""
import json
import os
from typing import Iterator

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()


def _bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=_bootstrap(),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
        linger_ms=20,
    )


def produce_json(topic: str, record: dict, key: str | None = None) -> None:
    """Шлёт одну JSON-запись. Блокирует до подтверждения."""
    producer = get_producer()
    try:
        future = producer.send(topic, value=record, key=key)
        future.get(timeout=10)
    finally:
        producer.flush()
        producer.close()


def consume_json(
    topic: str,
    group_id: str,
    from_beginning: bool = False,
    timeout_ms: int | None = None,
) -> Iterator[dict]:
    """
    Итератор по JSON-записям. timeout_ms=None — бесконечно ждёт новых сообщений.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=_bootstrap(),
        group_id=group_id,
        auto_offset_reset="earliest" if from_beginning else "latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms or float("inf"),
    )
    try:
        for msg in consumer:
            yield msg.value
    finally:
        consumer.close()


def ensure_topic(name: str, partitions: int = 1, replication: int = 1) -> None:
    """Создаёт топик если его ещё нет. Идемпотентно."""
    admin = KafkaAdminClient(bootstrap_servers=_bootstrap())
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=partitions, replication_factor=replication)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def list_topics() -> list[str]:
    admin = KafkaAdminClient(bootstrap_servers=_bootstrap())
    try:
        return sorted(admin.list_topics())
    finally:
        admin.close()
