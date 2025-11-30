import argparse
import json
import time
from typing import Dict, Any, Optional, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.crawler.utils.logger import CrawlerLogger
from src.crawler.storage.database_service import DatabaseService
from src.crawler.core.html_downloader import HTMLDownloader
from src.crawler.parsing.link_extractor import LinkExtractor
from src.crawler.utils.property_matcher import PropertyURLMatcher
from pybloom_live import BloomFilter


class Worker:
    """Worker that consumes URLs from a Kafka topic and processes them."""

    def __init__(self, topic: str, config_path: str = "config.json") -> None:
        self.logger = CrawlerLogger()
        self.bloom = BloomFilter(error_rate=0.001, capacity=1000000)
        self.topic = topic

        with open(config_path, 'r') as f:
            self.config: Dict[str, Any] = json.load(f)

        bootstrap_servers: List[str] = self.config.get(
            'kafka', {}).get(
            'bootstrap_servers', ['localhost:9092'])

        self.logger.log_info(f"Bootstrap servers: {bootstrap_servers}")

        priority = topic.split('_')[-1]
        consumer_group = f"crawler-group-{priority}"

        max_retries = 30
        retry_delay = 2
        last_error = None

        for attempt in range(max_retries):
            try:
                self.consumer: KafkaConsumer = KafkaConsumer(
                    bootstrap_servers=bootstrap_servers,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    enable_auto_commit=False,
                    auto_offset_reset='earliest',
                    max_poll_records=1,
                    fetch_min_bytes=1,
                    fetch_max_wait_ms=100,
                    max_poll_interval_ms=300000,
                    group_id=consumer_group
                )

                # self.consumer.assign([partition])
                self.logger.log_info(
                    f"Connected to Kafka on attempt {attempt + 1}")
                self.logger.log_info(f"Consumer group: {consumer_group}")
                break

            except NoBrokersAvailable as e:
                last_error = e
                if attempt < max_retries - 1:
                    self.logger.log_warning(
                        f"Kafka not ready, retrying in {retry_delay}s... ({attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    raise

        # Initialize producer with retry/backoff so workers can re-enqueue
        # discovered links
        self.bootstrap_servers: List[str] = bootstrap_servers
        backoff_seconds: List[int] = [1, 2, 4, 8, 15, 30]
        last_error: Optional[Exception] = None
        self.producer: Optional[KafkaProducer] = None
        for delay in backoff_seconds:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    api_version=(0, 11, 5),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    max_in_flight_requests_per_connection=1
                )
                break
            except NoBrokersAvailable as e:
                last_error = e
                self.logger.log_warning(
                    f"Kafka broker not available for producer. Retrying in {delay}s...")
                time.sleep(delay)
        if self.producer is None:
            raise last_error if last_error else RuntimeError(
                "Failed to create KafkaProducer")

        # Initialize database service
        db_cfg: Dict[str, Any] = self.config.get('database', {})
        self.database_service: DatabaseService = DatabaseService(
            connection_string=db_cfg.get('connection_string', ''),
            database_name=db_cfg.get('database_name', 'crawler'),
            output_dir=self.config.get('output_dir', 'crawled_data')
        )
        self.downloader: HTMLDownloader = HTMLDownloader(
            timeout=self.config.get('timeout', 30),
            max_retries=self.config.get('max_retries', 3),
            delay=self.config.get('delay_between_requests', 2.0),
            min_request_interval=self.config.get('min_request_interval', 5.0),
            headless=self.config.get('headless', True)
        )
        self.link_extractor: LinkExtractor = LinkExtractor()
        self.prioritizer: Prioritizer = Prioritizer()
        self.property_matcher: PropertyURLMatcher = PropertyURLMatcher()
        self._topic_for_priority = lambda p: f"urls_priority_{p}"

    def process_url(
            self, url: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        start: float = time.time()
        self.logger.log_info(
            f"================================================")
        self.logger.log_info(f"PROCESSING URL: {url}")
        self.logger.log_info(
            f"================================================")

        try:
            # Initialize payload if None
            if payload is None:
                payload = {}

            if url in self.bloom:
                self.logger.log_info(f"URL already processed: {url}")
                return True

            if self.database_service.get_page(url) is not None:
                self.logger.log_info(f"URL already visited: {url}")
                return True

            self.logger.set_current_url(url)
            self.logger.log_info(f"Processing URL from topic: {url}")
            result: Optional[Dict[str, Any]] = self.downloader.download(url)

            if not result:
                self.logger.log_error("Download failed")
                return False

            # Check if this is a property page
            is_property = self.property_matcher.is_property_url(url)

            # Only save property pages to database
            if is_property:
                self.logger.log_info(
                    f"Property page detected - saving to database: {url}")
                property_data: dict[str, Any] = self.database_service.save_page(
                    url=url,
                    html_content=result['content'],
                    status_code=result['status_code'],
                    crawl_duration=time.time() - start,
                )

                if not property_data:
                    self.logger.log_error(
                        "Failed to save property page to database")
                    return False
            else:
                self.logger.log_info(
                    f"Listing page detected - extracting links only (not saving): {url}")

            # Get current depth from payload
            self.bloom.add(url)
            self.logger.log_info(f"Added URL to bloom filter: {url}")
            links = self.link_extractor.extract_links(result['content'], url)
            enqueued: int = 0
            self.logger.log_info(f"Extracted {len(links)} links from {url}")

            for link in links:
                if not self.property_matcher.is_relevant_url(link):
                    self.logger.log_info(
                        f"Skipping URL (not relevant): {link}")
                    continue

                self.logger.log_info(
                    f"Checking bloom filter for link {link} , Is Link in bloom? {link in self.bloom}")
                if link in self.bloom:
                    self.logger.log_info(
                        f"Skipping URL (already processed): {link}")
                    continue

                self.logger.log_info(f"Assigning priority to link {link}")
                priority: int = self.prioritizer.assign_priority(link)
                self.logger.log_info(
                    f"Priority assigned to link {link}: {priority}")
                if priority == -1:
                    self.logger.log_info(
                        f"Skipping URL (non-target domain): {link}")
                    continue

                topic: str = self._topic_for_priority(priority)
                self.logger.log_info(f"Sending link to topic: {topic}")
                try:
                    try:
                        # Send extracted links back to the same queue
                        # FIFO ordering ensures they're processed after current
                        # messages
                        self.producer.send(topic, value={
                            "url": link,
                            "priority": priority,
                            "ts": time.time(),
                            "source": "extracted"
                        })
                        self.logger.log_info(f"Sent link to topic: {topic}")
                        enqueued += 1
                    except Exception as e:
                        self.logger.log_warning(
                            f"Failed to enqueue link {link}: {e}")
                        raise e
                    self.bloom.add(link)
                    self.logger.log_info(f"Added link to bloom filter: {link}")
                except Exception as e:
                    self.logger.log_warning(
                        f"Error Occurred while processing link {link}: {e}")

            if enqueued:
                # Flush immediately - the age-based processing in the consumer
                # will ensure proper FIFO ordering by waiting before processing
                # newly extracted links
                self.producer.flush()
                self.logger.log_info(
                    f"Enqueued {enqueued} links for further crawling (will be aged before processing)")
            return True
        except Exception as e:
            self.logger.log_error(f"Worker error: {e}")
            return False
        finally:
            self.logger.clear_current_url()

    def run(self) -> None:
        """Run the worker in standby mode - connected but only processing when URLs are available."""
        self.logger.log_info(
            f"================================================")
        self.logger.log_info(f"WORKER BOOTING UP...")
        self.logger.log_info(
            f"================================================")
        time.sleep(60)
        self.logger.log_info(f"WORKER BOOTED UP...")
        self.logger.log_info(
            f"================================================")
        self.logger.log_info(
            f"Worker connected to topic and ready to process URLs")
        self.logger.log_info(
            f"================================================")
        self.logger.log_info(
            "Worker is in standby mode - will process URLs after subscribing to topic")
        self.logger.log_info(
            "Using STRICT FIFO ordering - processing messages in exact order received")
        self.logger.log_info(f"Topic: {self.topic}")
        self.logger.log_info(f"Subscribing to topic: {self.topic}")
        self.consumer.subscribe([self.topic])
        self.logger.log_info(f"Subscribed to topic: {self.topic}")
        self.logger.log_info(
            f"================================================")

        try:
            for msg in self.consumer:
                payload: Dict[str, Any] = msg.value
                url: Optional[str] = payload.get('url')

                if not url:
                    # Commit even for invalid messages to move forward
                    self.consumer.commit()
                    continue

                self.process_url(url, payload)

                try:
                    self.consumer.commit()
                    self.logger.log_info(
                        f"[FIFO] Committed offset {msg.offset}")
                except Exception as e:
                    self.logger.log_error(f"Failed to commit offset: {e}")
                    # On commit failure, the message may be reprocessed

        except KeyboardInterrupt:
            self.logger.log_info("Worker shutting down...")
        finally:
            self.database_service.close()
            self.consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Priority worker')
    parser.add_argument(
        'topic',
        help='Kafka topic to consume (e.g., urls_priority_5)')
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to config.json')
    args = parser.parse_args()

    worker = Worker(topic=args.topic, config_path=args.config)
    worker.run()


if __name__ == "__main__":
    main()
