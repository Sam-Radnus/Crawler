import argparse
import json
import time
from typing import Optional

from kafka import KafkaProducer

from src.crawler.prioritizer import Prioritizer


def topic_for_priority(priority: int) -> str:
    return f"urls_priority_{priority}"


def publish(url: str, priority: Optional[int], bootstrap_servers: str) -> None:
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    if priority is None:
        priority = Prioritizer().assign_priority(url)
    topic = topic_for_priority(priority)
    payload = {"url": url, "priority": priority, "ts": time.time()}
    producer.send(topic, payload)
    producer.flush()
    print(f"Published to {topic}: {payload}")


def main():
    parser = argparse.ArgumentParser(description="Publish a URL to a priority topic")
    parser.add_argument("url", help="URL to publish")
    parser.add_argument("--priority", type=int, choices=[1, 2, 3, 4, 5], help="Priority 1-5 (5 highest). If omitted, random")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server (host:port)")
    args = parser.parse_args()

    publish(args.url, args.priority, args.bootstrap)


if __name__ == "__main__":
    main()


