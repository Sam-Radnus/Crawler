import json
import time
from typing import List
from kafka import KafkaProducer

from src.crawler.prioritizer import Prioritizer


class MasterDispatcher:
    """Fetch URLs and dispatch to Kafka priority topics."""

    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.bootstrap_servers = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])
        self.seed_urls: List[str] = self.config.get('seed_urls', [])
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            api_version=(0,11,5),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        self.prioritizer = Prioritizer()

    def _topic_for_priority(self, priority: int) -> str:
        # Topic naming: urls_priority_{priority}
        return f"urls_priority_{priority}"

    def dispatch(self) -> None:
        for url in self.seed_urls:
            priority = self.prioritizer.assign_priority(url)
            topic = self._topic_for_priority(priority)
            self.producer.send(topic, {"url": url, "priority": priority, "ts": time.time()})
        self.producer.flush()


def main():
    dispatcher = MasterDispatcher()
    dispatcher.dispatch()


if __name__ == "__main__":
    main()


