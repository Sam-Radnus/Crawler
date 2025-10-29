"""
Master Dispatcher - Dispatches seed URLs to Kafka priority queues
"""

import json
import time
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from geospatial.prioritizer import Prioritizer
from src.crawler.robots_checker import RobotsChecker
from urllib.parse import urlparse


class MasterDispatcher:
    """Dispatches seed URLs from config to appropriate Kafka priority queues."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize master dispatcher."""
        with open(config_path, 'r') as f:
            self.config: Dict[str, Any] = json.load(f)
        
        self.bootstrap_servers: List[str] = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])
        
        # Initialize Kafka producer
        self.producer: KafkaProducer = self._create_producer()
        self.prioritizer = Prioritizer()
        
        # Initialize robots.txt checker if enabled
        respect_robots = self.config.get('respect_robots', True)
        self.robots_checker = RobotsChecker(user_agent="WebCrawler/1.0") if respect_robots else None
    
    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer with retry logic."""
        backoff_seconds: List[int] = [1, 2, 4, 8, 15, 30]
        last_error: Exception | None = None
        
        for delay in backoff_seconds:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    api_version=(0, 11, 5),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    max_in_flight_requests_per_connection=1  # Critical: guarantees order per partition
                )
                return producer
            except NoBrokersAvailable as e:
                last_error = e
                print(f"⚠️  Kafka broker not available. Retrying in {delay}s...")
                time.sleep(delay)
        
        raise last_error if last_error else RuntimeError("Failed to create KafkaProducer")
    
    def _topic_for_priority(self, priority: int) -> str:
        """Get topic name for priority level."""
        return f"urls_priority_{priority}"
    
    def dispatch(self) -> None:
        """Dispatch seed URLs from config to Kafka topics."""
        seed_urls: List[str] = self.config.get('seed_urls', [])
        
        if not seed_urls:
            print("⚠️  No seed URLs found in config")
            return
        
        print(f"📤 Dispatching {len(seed_urls)} seed URLs to Kafka topics...")
        
        dispatched = 0
        skipped = 0
        
        for url in seed_urls:
            try:
                # Check robots.txt if enabled
                if self.robots_checker and not self.robots_checker.can_fetch(url):
                    print(f"⚠️  Skipping URL (disallowed by robots.txt): {url}")
                    skipped += 1
                    continue
                
                # Assign priority using prioritizer
                priority = self.prioritizer.assign_priority(url)
                
                # Skip if priority is -1 (not from target domain or invalid)
                if priority == -1:
                    print(f"⚠️  Skipping URL (not from target domain): {url}")
                    skipped += 1
                    continue
                
                # Determine topic
                topic = self._topic_for_priority(priority)
                
                # Create message payload
                payload = {
                    "url": url,
                    "priority": priority,
                    "timestamp": int(time.time() * 1000),
                    "ts": time.time(),
                    "source": "seed",
                    "queued_at": time.time()
                }
                
                # Send to Kafka
                parsed = urlparse(url)
                domain = parsed.netloc
                self.producer.send(
                    topic,
                    value=payload,
                    key=domain.encode('utf-8') if domain else None
                )
                
                print(f"✅ Dispatched to {topic} (priority {priority}): {url}")
                dispatched += 1
                
            except Exception as e:
                print(f"❌ Failed to dispatch URL {url}: {e}")
                skipped += 1
        
        # Flush producer to ensure all messages are sent
        self.producer.flush()
        
        print(f"\n📊 Dispatch Summary:")
        print(f"   ✅ Dispatched: {dispatched}")
        print(f"   ⚠️  Skipped: {skipped}")
        print(f"   📋 Total: {len(seed_urls)}")
    
    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.close()

