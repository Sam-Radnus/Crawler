"""
URL Queue Manager - Manages manual URL additions to specific queues
"""

import json
import time
from typing import Dict, Any, List, Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from geospatial.prioritizer import Prioritizer
from src.crawler.robots_checker import RobotsChecker
from urllib.parse import urlparse


class URLQueueManager:
    """Manages manual URL additions to Kafka priority queues."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize URL queue manager."""
        with open(config_path, 'r') as f:
            self.config: Dict[str, Any] = json.load(f)
        
        self.bootstrap_servers: List[str] = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])
        self.default_priority = self.config.get('default_priority', 3)  # Default queue priority
        
        # Initialize Kafka producer
        self.producer: KafkaProducer = self._create_producer()
        self.prioritizer = Prioritizer()
        
        # Target domains
        self.target_domains = {'realtor.com', 'www.realtor.com', 'zillow.com', 'www.zillow.com'}
        
        # Initialize robots.txt checker
        respect_robots = self.config.get('respect_robots', True)
        self.robots_checker = RobotsChecker(user_agent="WebCrawler/1.0") if respect_robots else None
    
    def _create_producer(self) -> KafkaProducer:
        """Create Kafka producer with retry logic."""
        import logging
        logging.basicConfig(level=logging.INFO)
        
        backoff_seconds: List[int] = [1, 2, 4, 8, 15, 30]
        last_error: Optional[Exception] = None
        
        for delay in backoff_seconds:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    api_version=(0, 11, 5),
                    max_in_flight_requests_per_connection=1  # Critical: guarantees order per partition
                )
                return producer
            except NoBrokersAvailable as e:
                last_error = e
                logging.warning(f"Kafka broker not available. Retrying in {delay}s...")
                time.sleep(delay)
        
        raise last_error if last_error else RuntimeError("Failed to create KafkaProducer")
    
    def _topic_for_priority(self, priority: int) -> str:
        """Get topic name for priority level."""
        return f"urls_priority_{priority}"
    
    def add_url(self, url: str, priority: Optional[int] = None, queue: Optional[str] = None) -> bool:
        """
        Add a URL to a specific queue.
        
        Args:
            url: URL to add
            priority: Priority level (1-5). If None, uses default_priority
            queue: Specific queue name (e.g., 'urls_priority_3'). If None, uses priority-based topic
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if URL is from target domain
            parsed = urlparse(url)
            if parsed.netloc.lower() not in self.target_domains:
                print(f"❌ URL rejected - not from target domain: {url}")
                return False
            
            # Check robots.txt if enabled
            if self.robots_checker and not self.robots_checker.can_fetch(url):
                print(f"❌ URL rejected - disallowed by robots.txt: {url}")
                return False
            # Determine priority
            if priority is None:
                priority = self.default_priority
            
            # Validate priority
            if not 1 <= priority <= 5:
                raise ValueError(f"Priority must be between 1 and 5, got {priority}")
            
            # Determine topic
            if queue:
                topic = queue
            else:
                topic = self._topic_for_priority(priority)
            
            # Create message payload  
            # Mark as 'manual' source so workers process immediately without aging delay
            payload = {
                "url": url,
                "priority": priority,
                "timestamp": int(time.time() * 1000),
                "ts": time.time(),
                "source": "manual",
                "queued_at": time.time()
            }
            
            # Send to Kafka
            domain = parsed.netloc
            self.producer.send(
                topic, 
                payload,
                key=domain.encode('utf-8')
            )
            self.producer.send(topic, payload, key=domain.encode('utf-8'))
            self.producer.flush()
            
            print(f"✅ Added URL to {topic}: {url}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to add URL {url}: {e}")
            return False
    
    def add_urls_batch(self, urls: List[str], priority: Optional[int] = None, queue: Optional[str] = None) -> Dict[str, bool]:
        """
        Add multiple URLs to a queue.
        
        Args:
            urls: List of URLs to add
            priority: Priority level (1-5). If None, uses default_priority
            queue: Specific queue name. If None, uses priority-based topic
            
        Returns:
            Dict mapping URLs to success status
        """
        results = {}
        
        for url in urls:
            success = self.add_url(url, priority, queue)
            results[url] = success
        
        return results
    
    def get_queue_info(self) -> Dict[str, Any]:
        """Get information about available queues."""
        return {
            "available_queues": [f"urls_priority_{i}" for i in range(1, 6)],
            "default_priority": self.default_priority,
            "priority_range": "1-5 (5 = highest priority)",
            "kafka_servers": self.bootstrap_servers
        }
    
    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.close()


def main():
    """Command-line interface for URL queue management."""
    import argparse
    
    parser = argparse.ArgumentParser(description='URL Queue Manager')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Add URL command
    add_parser = subparsers.add_parser('add', help='Add URL(s) to queue')
    add_parser.add_argument('urls', nargs='+', help='URL(s) to add')
    add_parser.add_argument('--priority', '-p', type=int, choices=range(1, 6), 
                           help='Priority level (1-5, default from config)')
    add_parser.add_argument('--queue', '-q', help='Specific queue name (e.g., urls_priority_3)')
    
    # Batch add command
    batch_parser = subparsers.add_parser('batch', help='Add multiple URLs from file')
    batch_parser.add_argument('file', help='File containing URLs (one per line)')
    batch_parser.add_argument('--priority', '-p', type=int, choices=range(1, 6),
                             help='Priority level (1-5, default from config)')
    batch_parser.add_argument('--queue', '-q', help='Specific queue name')
    
    # Info command
    subparsers.add_parser('info', help='Show queue information')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        manager = URLQueueManager()
        
        if args.command == 'add':
            results = manager.add_urls_batch(args.urls, args.priority, args.queue)
            success_count = sum(1 for success in results.values() if success)
            print(f"\n📊 Results: {success_count}/{len(results)} URLs added successfully")
            
        elif args.command == 'batch':
            with open(args.file, 'r') as f:
                urls = [line.strip() for line in f if line.strip()]
            
            results = manager.add_urls_batch(urls, args.priority, args.queue)
            success_count = sum(1 for success in results.values() if success)
            print(f"\n📊 Batch Results: {success_count}/{len(results)} URLs added successfully")
            
        elif args.command == 'info':
            info = manager.get_queue_info()
            print("🔍 Queue Information:")
            print(f"Available queues: {', '.join(info['available_queues'])}")
            print(f"Default priority: {info['default_priority']}")
            print(f"Priority range: {info['priority_range']}")
            print(f"Kafka servers: {', '.join(info['kafka_servers'])}")
        
        manager.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
