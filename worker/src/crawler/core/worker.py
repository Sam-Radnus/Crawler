import argparse
import json
import time
from typing import Callable
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.crawler.utils.logger import CrawlerLogger
from src.crawler.storage.content_storage import ContentStorage
from src.crawler.core.html_downloader import HTMLDownloader
from src.crawler.parsing.link_extractor import LinkExtractor
from src.crawler.prioritizer import Prioritizer


class Worker:
    """Worker that consumes URLs from a Kafka topic and processes them."""

    def __init__(self, topic: str, group_id: str = "crawler_workers", config_path: str = "config.json"):
        self.logger = CrawlerLogger()
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        bootstrap_servers = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])

        self.consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            auto_offset_reset='earliest',
        )

        # Initialize producer with retry/backoff so workers can re-enqueue discovered links
        self.bootstrap_servers = bootstrap_servers
        backoff_seconds = [1, 2, 4, 8, 15, 30]
        last_error = None
        self.producer = None
        for delay in backoff_seconds:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    api_version=(0, 11, 5),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                break
            except NoBrokersAvailable as e:
                last_error = e
                self.logger.log_warning(f"Kafka broker not available for producer. Retrying in {delay}s...")
                time.sleep(delay)
        if self.producer is None:
            raise last_error if last_error else RuntimeError("Failed to create KafkaProducer")

        # Reuse existing components for processing
        db_cfg = self.config.get('database', {})
        self.content_storage = ContentStorage(
            output_dir=self.config.get('output_dir', 'crawled_data'),
            connection_string=db_cfg.get('connection_string', 'mongodb://localhost:27017/'),
            database_name=db_cfg.get('database_name', 'web_crawler')
        )
        self.downloader = HTMLDownloader(
            timeout=self.config.get('timeout', 10),
            max_retries=self.config.get('max_retries', 3),
            delay=self.config.get('delay_between_requests', 1.0),
            user_agent=self.config.get('user_agent', 'WebCrawler/1.0')
        )
        self.link_extractor = LinkExtractor()
        self.prioritizer = Prioritizer()
        self._topic_for_priority = lambda p: f"urls_priority_{p}"

    def process_url(self, url: str) -> bool:
        start = time.time()
        try:
            self.logger.set_current_url(url)
            self.logger.log_info(f"Processing URL from topic: {url}")
            result = self.downloader.download(url)
            if not result:
                self.logger.log_error("Download failed")
                return False
            ok = self.content_storage.save_page(
                url=url,
                html_content=result['content'],
                status_code=result['status_code'],
                headers=result['headers'],
                crawl_duration=time.time() - start,
            )
            if not ok:
                self.logger.log_error("Save failed")
                return False
            # Extract links and enqueue them back to appropriate priority topics
            links = self.link_extractor.extract_links(result['content'], url)
            enqueued = 0
            for link in links:
                priority = self.prioritizer.assign_priority(link)
                topic = self._topic_for_priority(priority)
                try:
                    self.producer.send(topic, {"url": link, "priority": priority, "ts": time.time()})
                    enqueued += 1
                except Exception as e:
                    self.logger.log_warning(f"Failed to enqueue link {link}: {e}")
            if enqueued:
                self.producer.flush()
            return True
        except Exception as e:
            self.logger.log_error(f"Worker error: {e}")
            return False
        finally:
            self.logger.clear_current_url()

    def run(self) -> None:
        for msg in self.consumer:
            payload = msg.value
            url = payload.get('url')
            if not url:
                continue
            self.process_url(url)


def main():
    parser = argparse.ArgumentParser(description='Priority worker')
    parser.add_argument('topic', help='Kafka topic to consume (e.g., urls_priority_5)')
    parser.add_argument('--group', default='crawler_workers', help='Kafka consumer group id')
    parser.add_argument('--config', default='config.json', help='Path to config.json')
    args = parser.parse_args()

    worker = Worker(topic=args.topic, group_id=args.group, config_path=args.config)
    worker.run()


if __name__ == "__main__":
    main()


