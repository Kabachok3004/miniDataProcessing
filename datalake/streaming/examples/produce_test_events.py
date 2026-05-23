"""
Демо-продюсер: льёт фейковые события в топик `events`.

Запуск:
    export KAFKA_BOOTSTRAP=<vps_ip>:9094
    python -m datalake.streaming.examples.produce_test_events --rate 10 --duration 60

Формат записей совпадает с тем, что ожидает rolling_metrics.py:
    {event_type, user_id, amount, ts_ms}
"""
import argparse
import random
import time

from datalake.connectors.kafka_client import ensure_topic, get_producer


EVENT_TYPES = ["click", "view", "purchase", "signup"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--topic", default="events")
    parser.add_argument("--rate", type=float, default=5.0, help="событий/сек")
    parser.add_argument("--duration", type=float, default=30.0, help="секунд")
    args = parser.parse_args()

    ensure_topic(args.topic)
    producer = get_producer()

    interval = 1.0 / args.rate
    start = time.time()
    n = 0
    try:
        while time.time() - start < args.duration:
            record = {
                "event_type": random.choice(EVENT_TYPES),
                "user_id": random.randint(1, 1000),
                "amount": round(random.uniform(0.5, 100.0), 2),
                "ts_ms": int(time.time() * 1000),
            }
            producer.send(args.topic, value=record)
            n += 1
            if n % 50 == 0:
                print(f"sent {n} events", flush=True)
            time.sleep(interval)
    finally:
        producer.flush()
        producer.close()
        print(f"Готово: отправлено {n} событий в '{args.topic}'.")


if __name__ == "__main__":
    main()
