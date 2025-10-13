import json
import time
from typing import List, Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging
import os

from src.crawler.prioritizer import Prioritizer
from src.crawler.robots_checker import RobotsChecker


class MasterDispatcher:
    """Fetch URLs and dispatch to Kafka priority topics."""

    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r') as f:
            self.config: Dict[str, Any] = json.load(f)
        self.bootstrap_servers: List[str] = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])
        self.seed_urls: List[str] = self.config.get('seed_urls', [])
        # Initialize KafkaProducer with extended retry/backoff to allow broker readiness
        logging.basicConfig(level=logging.INFO)
        backoff_seconds: List[int] = [1, 2, 4, 8, 15, 30, 45, 60]
        last_error: Optional[Exception] = None
        for delay in backoff_seconds:
            try:
                self.producer: KafkaProducer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    api_version=(0, 11, 5),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                break
            except NoBrokersAvailable as e:
                last_error = e
                logging.warning("Kafka broker not available yet at %s. Retrying in %s seconds...", self.bootstrap_servers, delay)
                time.sleep(delay)
        else:
            # If loop didn't break, raise the last error
            raise last_error
        self.prioritizer = Prioritizer()
        
        # Initialize robots.txt checker
        respect_robots = self.config.get('respect_robots', True)
        self.robots_checker = RobotsChecker(user_agent="WebCrawler/1.0") if respect_robots else None
        
        if self.robots_checker:
            logging.info("Robots.txt checking enabled")
        else:
            logging.info("Robots.txt checking disabled")

    def _topic_for_priority(self, priority: int) -> str:
        return f"urls_priority_{priority}"

    def dispatch(self) -> None:
        for url in self.seed_urls:
            # Check robots.txt if enabled
            if self.robots_checker and not self.robots_checker.can_fetch(url):
                logging.info(f"Skipping URL (disallowed by robots.txt): {url}")
                continue
            
            priority: Optional[int] = self.prioritizer.assign_priority(url)
            
            # Skip if URL is from non-target domain
            if priority is None:
                logging.info(f"Skipping URL (non-target domain): {url}")
                continue
            
            topic: str = self._topic_for_priority(priority)
            self.producer.send(topic, {"url": url, "priority": priority, "ts": time.time()})
            logging.info(f"Dispatched {url} to {topic}")
        
        self.producer.flush() # guarantees delivery of messages to kafka broker


def main() -> None:
    """Main entry point for master dispatcher."""
    dispatcher = MasterDispatcher()
    interval_str: str = os.getenv("DISPATCH_INTERVAL_SECONDS", "0")
    try:
        interval: float = float(interval_str)
    except ValueError:
        interval: float = 0.0

    # Check if running in controlled mode (no automatic dispatch)
    controlled_mode = os.getenv("CONTROLLED_MODE", "false").lower() == "true"
    
    if controlled_mode:
        logging.info("Master dispatcher running in controlled mode - waiting for external trigger")
        # In controlled mode, just keep the process alive
        # The CLI will trigger dispatch via direct instantiation
        try:
            while True:
                time.sleep(60)  # Sleep for 1 minute intervals
                logging.debug("Master dispatcher still alive in controlled mode")
        except KeyboardInterrupt:
            logging.info("Master dispatcher shutting down")
            return

    if interval <= 0:
        dispatcher.dispatch()
        return

    logging.info("Master dispatcher running in periodic mode every %s seconds", interval)
    while True:
        start: float = time.time()
        dispatcher.dispatch()
        elapsed: float = time.time() - start
        sleep_for: float = max(0.0, interval - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()


