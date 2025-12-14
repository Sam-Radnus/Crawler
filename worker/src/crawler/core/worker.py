import argparse
import json
import time
from typing import Dict, Any, Optional, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from src.crawler.utils.logger import CrawlerLogger
from geospatial.prioritizer import Prioritizer
from src.crawler.storage.database_service import DatabaseService
from src.crawler.storage.cache_service import RedisService
from src.crawler.core.html_downloader import HTMLDownloader
from src.crawler.core.transaction import (
    TransactionManager,
    TransactionStatus
)

from src.crawler.parsing.link_extractor import LinkExtractor
from src.crawler.utils.property_matcher import PropertyURLMatcher
from pybloom_live import BloomFilter

import os
from src.crawler.storage.file_storage import (
    extract_property_id,
    ensure_dirs,
    save_html,
    extract_image_urls,
    download_images,
)


class PropertyPageTransactionManager:
    def __init__(
        self,
        url: str,
        content: str,
        status_code: int,
        database_service,
        cache_service,
        logger,
        producer,
        dlq_topic: str,
        start_time: float
    ):
        self.url = url
        self.content = content
        self.status_code = status_code
        self.database_service = database_service
        self.cache = cache_service
        self.logger = logger
        self.producer = producer
        self.dlq_topic = dlq_topic
        self.start_time = start_time
        
        self.property_id = None
        self.base_path = None
        self.images_path = None
        self.html_saved = False
        self.db_saved = False
        self.images_downloaded = False
        self.cache_updated = False
        self.property_data = None
    
    def _setup_directories(self):
        self.property_id = extract_property_id(self.url)
        self.base_path = os.path.join("/mnt/storage", self.property_id)
        os.makedirs(self.base_path, exist_ok=True)
        self.images_path = ensure_dirs(self.base_path)
        self.logger.log_info(f"Created directories: {self.base_path}")
        return self.base_path
    
    def _rollback_setup_directories(self):
        if self.base_path and os.path.exists(self.base_path):
            try:
                if not os.listdir(self.base_path):
                    os.rmdir(self.base_path)
                    self.logger.log_info(f"Rolled back directory: {self.base_path}")
            except Exception as e:
                self.logger.log_warning(f"Failed to rollback directory: {e}")
    
    def _save_html(self):
        save_html(self.base_path, self.content)
        self.html_saved = True
        self.logger.log_info(f"Saved HTML to {self.base_path}/index.html")
        return f"{self.base_path}/index.html"
    
    def _rollback_save_html(self):
        if self.html_saved:
            html_path = os.path.join(self.base_path, "index.html")
            try:
                if os.path.exists(html_path):
                    os.remove(html_path)
                    self.logger.log_info(f"Rolled back HTML file: {html_path}")
            except Exception as e:
                self.logger.log_warning(f"Failed to rollback HTML: {e}")
    
    def _save_to_database(self):
        self.property_data = self.database_service.save_page(
            url=self.url,
            html_content=self.content,
            status_code=self.status_code,
            storage_path=self.base_path,
            crawl_duration=time.time() - self.start_time,
        )
        
        if not self.property_data:
            raise Exception("Failed to save property data to database")
        
        self.db_saved = True
        self.logger.log_info(f"Saved property data to database")
        return self.property_data
    
    def _rollback_save_to_database(self):
        if self.db_saved:
            try:
                self.database_service.delete_page(self.url)
                self.logger.log_info(f"Rolled back database entry for {self.url}")
            except Exception as e:
                self.logger.log_warning(f"Failed to rollback database: {e}")
    
    def _update_cache(self):
        self.cache.store_url_hash(self.url, self.content)
        self.cache_updated = True
        self.logger.log_info(f"Updated cache hash for {self.url}")
        return True
    
    def _rollback_update_cache(self):
        if self.cache_updated:
            try:
                self.cache.delete_url_hash(self.url)
                self.logger.log_info(f"Rolled back cache hash for {self.url}")
            except Exception as e:
                self.logger.log_warning(f"Failed to rollback cache: {e}")
    
    def _download_images(self):
        image_urls = extract_image_urls(self.content, self.url)
        self.logger.log_info(f"Found {len(image_urls)} images")
        
        if image_urls:
            download_images(image_urls, self.images_path)
            self.images_downloaded = True
            self.logger.log_info(f"Downloaded {len(image_urls)} images to {self.base_path}/images")
        
        return len(image_urls)
    
    def _rollback_download_images(self):
        if self.images_downloaded and self.images_path:
            try:
                if os.path.exists(self.images_path):
                    import shutil
                    shutil.rmtree(self.images_path)
                    self.logger.log_info(f"Rolled back images directory: {self.images_path}")
            except Exception as e:
                self.logger.log_warning(f"Failed to rollback images: {e}")
    
    def execute(self) -> bool:
        # Check if content changed before starting transaction
        if not self.cache.has_url_changed(self.url, self.content):
            self.logger.log_info(f"Content unchanged for {self.url}, skipping")
            return True
        
        transaction_id = f"property:{self.property_id if self.property_id else hash(self.url)}:{time.time()}"
        tm = TransactionManager(self.cache, transaction_id)
        
        tm.add_step(
            "setup_directories",
            self._setup_directories,
            self._rollback_setup_directories
        ).add_step(
            "save_html",
            self._save_html,
            self._rollback_save_html
        ).add_step(
            "save_to_database",
            self._save_to_database,
            self._rollback_save_to_database
        ).add_step(
            "update_cache",
            self._update_cache,
            self._rollback_update_cache
        ).add_step(
            "download_images",
            self._download_images,
            self._rollback_download_images
        )
        
        result = tm.execute()
        
        if result.status == TransactionStatus.COMPLETED:
            self.logger.log_info(f"Successfully processed property page: {self.url}")
            return True
        else:
            self.logger.log_error(
                f"Transaction failed for {self.url}: {result.error}"
            )
            self.logger.log_error(
                f"Failed at step: {result.failed_step}, completed: {result.completed_steps}"
            )
            
            # Send to DLQ
            try:
                self.producer.send(self.dlq_topic, value={
                    "url": self.url,
                    "priority": 0,
                    "ts": time.time(),
                    "source": "transaction_failure",
                    "error": str(result.error),
                    "failed_step": result.failed_step,
                    "retry_count": result.retry_count
                })
                self.logger.log_info(f"Sent {self.url} to DLQ for retry")
            except Exception as e:
                self.logger.log_error(f"Failed to send to DLQ: {e}")
            
            return False


class Worker:
    """Worker that consumes URLs from a Kafka topic and processes them."""

    def __init__(self, topic: str, config_path: str = "config.json", cache_service = RedisService(host = "redis", port = 6379), dlq = "dlq") -> None:
        self.logger = CrawlerLogger()
        self.bloom = BloomFilter(error_rate=0.001, capacity=1000000)
        self.topic = topic
        self.cache = cache_service
        self.dlq_topic = dlq 

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

        # Initialize producer with retry/backoff
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
            if payload is None:
                payload = {}
                
            if url in self.bloom:
                self.logger.log_info(f"URL already processed: {url}")
                return True

            self.logger.set_current_url(url)
            self.logger.log_info(f"Processing URL from topic: {url}")
            result: Optional[Dict[str, Any]] = self.downloader.download(url)

            if not result:
                self.logger.log_error("Download failed")
                return False

            is_property = self.property_matcher.is_property_url(url)

            if is_property:
                self.logger.log_info(
                    f"Property page detected - saving to database: {url}")
                
                txn_manager = PropertyPageTransactionManager(
                    url = url,
                    content = result["content"],
                    status_code = result["status_code"],
                    database_service = self.database_service,
                    cache_service = self.cache,
                    logger = self.logger,
                    producer = self.producer,
                    dlq_topic = self.dlq_topic,
                    start_time = start
                )
                
                success = txn_manager.execute()
                
                if not success:
                    self.logger.log_error(f"Transaction failed for property page: {url}")
                    return False
            else:
                self.logger.log_info(
                    f"Listing page detected - extracting links only (not saving): {url}")

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
                    f"Checking bloom filter for link {link}, Is Link in bloom? {link in self.bloom}")
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
                    self.producer.send(topic, value={
                        "url": link,
                        "priority": priority,
                        "ts": time.time(),
                        "source": "extracted"
                    })
                    self.logger.log_info(f"Sent link to topic: {topic}")
                    enqueued += 1
                    self.bloom.add(link)
                    self.logger.log_info(f"Added link to bloom filter: {link}")
                except Exception as e:
                    self.logger.log_warning(
                        f"Failed to enqueue link {link}: {e}")

            if enqueued:
                self.producer.flush()
                self.logger.log_info(
                    f"Enqueued {enqueued} links for further crawling")
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
                    self.consumer.commit()
                    continue

                self.process_url(url, payload)

                try:
                    self.consumer.commit()
                    self.logger.log_info(
                        f"[FIFO] Committed offset {msg.offset}")
                except Exception as e:
                    self.logger.log_error(f"Failed to commit offset: {e}")

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