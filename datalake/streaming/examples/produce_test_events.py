
import argparse
import random
import signal
import time

from datalake.connectors.kafka_client import ensure_topic, get_producer


EVENT_TYPES = ["click", "view", "purchase", "signup"]

_running = True


def _on_signal(sig, frame) -> None:  # noqa: ANN001
    global _running
    _running = False
    print("\nОстанавливаем продюсер…", flush=True)


def _produce_batch(producer, topic: str, rate: float, duration: float) -> int:
    """Отправляет события в течение duration секунд, возвращает кол-во."""
    interval = 1.0 / rate
    start = time.time()
    n = 0
    while _running and time.time() - start < duration:
        record = {
            "event_type": random.choice(EVENT_TYPES),
            "user_id": random.randint(1, 1000),
            "amount": round(random.uniform(0.5, 100.0), 2),
            "ts_ms": int(time.time() * 1000),
        }
        producer.send(topic, value=record)
        n += 1
        if n % 100 == 0:
            print(f"  sent {n} events", flush=True)
        time.sleep(interval)
    return n


def main() -> None:
    global _running

    parser = argparse.ArgumentParser(description=(__doc__ or "Демо-продюсер событий в Kafka").split("\n\n")[0])
    parser.add_argument("--topic", default="events")
    parser.add_argument("--rate", type=float, default=5.0, help="событий/сек")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="секунд на батч (игнорируется при --loop)")
    parser.add_argument("--loop", action="store_true",
                        help="крутить без остановки; Ctrl+C для выхода")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    ensure_topic(args.topic)
    producer = get_producer()

    total = 0
    try:
        if args.loop:
            print(f"Запускаем бесконечный поток в '{args.topic}' "
                  f"({args.rate} events/sec). Ctrl+C для остановки.", flush=True)
            batch = 0
            while _running:
                batch += 1
                print(f"--- батч {batch} ---", flush=True)
                total += _produce_batch(producer, args.topic, args.rate, duration=60)
                producer.flush()
        else:
            total = _produce_batch(producer, args.topic, args.rate, args.duration)
    finally:
        producer.flush()
        producer.close()
        print(f"Итого отправлено: {total} событий в '{args.topic}'.")


if __name__ == "__main__":
    main()
