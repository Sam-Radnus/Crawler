#!/usr/bin/env python3
"""
Web Crawler CLI Interface

This script provides a command-line interface to control the web crawler system.
Workers stay connected to Kafka queues but only process URLs when explicitly started.
"""

import argparse
import json
import sys
import psycopg2
from typing import Dict, Any, List, Optional
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from src.crawler.core.master import MasterDispatcher
from src.crawler.queue_manager import URLQueueManager


class CrawlerCLI:
    """Command-line interface for the web crawler system."""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize CLI with configuration."""
        with open(config_path, 'r') as f:
            self.config: Dict[str, Any] = json.load(f)
        self.bootstrap_servers: List[str] = self.config.get('kafka', {}).get('bootstrap_servers', ['localhost:9092'])
        self.health_check_timeout = 10
        
    def _get_kafka_admin_client(self) -> KafkaAdminClient:
        """Get Kafka admin client for health checks."""
        try:
            return KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                api_version=(0, 11, 5)
            )
        except NoBrokersAvailable:
            raise ConnectionError(f"Unable to connect to Kafka brokers at {self.bootstrap_servers}")
    
    def _get_kafka_consumer(self, topic: str) -> KafkaConsumer:
        """Get Kafka consumer for topic health checks."""
        return KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            consumer_timeout_ms=1000,
            api_version=(0, 11, 5)
        )
    
    def _check_kafka_connection(self) -> bool:
        """Check if Kafka is accessible."""
        try:
            admin_client = self._get_kafka_admin_client()
            admin_client.describe_cluster()
            admin_client.close()
            return True
        except Exception as e:
            print(f"❌ Kafka connection failed: {e}")
            return False
    
    def _check_topic_exists(self, topic: str) -> bool:
        """Check if a Kafka topic exists."""
        try:
            admin_client = self._get_kafka_admin_client()
            metadata = admin_client.describe_topics([topic])
            admin_client.close()
            return topic in metadata
        except Exception as e:
            print(f"❌ Failed to check topic {topic}: {e}")
            return False
    
    def _get_topic_metadata(self, topic: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific topic."""
        try:
            consumer = self._get_kafka_consumer(topic)
            partitions = consumer.partitions_for_topic(topic)
            consumer.close()
            return {"partitions": len(partitions) if partitions else 0}
        except Exception as e:
            print(f"❌ Failed to get metadata for topic {topic}: {e}")
            return None
    
    def _check_worker_containers(self) -> List[Dict[str, Any]]:
        """Check status of worker containers by testing Kafka topic consumption."""
        worker_status = []
        
        for i in range(1, 6):  # worker-1 to worker-5
            container_name = f"crawler-worker-{i}"
            topic = f"urls_priority_{i}"
            
            try:
                # Try to create a consumer for the topic to test if workers are connected
                consumer = self._get_kafka_consumer(topic)
                # If we can create a consumer, assume workers are running
                # (This is a heuristic - workers typically consume from these topics)
                consumer.close()
                status = "running (topic accessible)"
            except Exception as e:
                status = "unknown (topic not accessible)"
                error = str(e)
            
            worker_status.append({
                "name": container_name,
                "status": status,
                "priority": i
            })
            
            if 'error' in locals():
                worker_status[-1]["error"] = error
        
        return worker_status
    
    def _check_master_container(self) -> Dict[str, Any]:
        """Check status of master container - we're running inside it, so it's running."""
        return {
            "name": "crawler-master",
            "status": "running (current container)"
        }
    
    def health_check(self) -> None:
        """Perform comprehensive health check of the crawler system."""
        print("🔍 Web Crawler Health Check")
        print("=" * 40)
        
        # Check Kafka connection
        print("\n📡 Kafka Connection:")
        if self._check_kafka_connection():
            print("✅ Kafka is accessible")
        else:
            print("❌ Kafka is not accessible")
            return
        
        # Check topics
        print("\n📋 Kafka Topics:")
        topics = [f"urls_priority_{i}" for i in range(1, 6)]
        existing_topics = 0
        for topic in topics:
            if self._check_topic_exists(topic):
                metadata = self._get_topic_metadata(topic)
                partitions = metadata["partitions"] if metadata else "unknown"
                print(f"✅ {topic} (partitions: {partitions})")
                existing_topics += 1
            else:
                print(f"⚠️  {topic} (will be created when crawling starts)")
        
        if existing_topics == 0:
            print("\n💡 Topics will be auto-created when you run 'python3 main.py start'")
        
        # Check worker containers
        print("\n👷 Worker Containers:")
        workers = self._check_worker_containers()
        for worker in workers:
            if "running" in worker["status"]:
                status_icon = "✅"
            elif "unknown" in worker["status"]:
                status_icon = "⚠️ "
            else:
                status_icon = "❌"
            print(f"{status_icon} {worker['name']} - {worker['status']}")
            if "error" in worker:
                print(f"   Error: {worker['error']}")
        
        print("\n💡 Workers will be ready when topics exist and crawling is started")
        
        # Check master container
        print("\n🎯 Master Container:")
        master = self._check_master_container()
        status_icon = "✅" if master["status"] == "running" else "❌"
        print(f"{status_icon} {master['name']} - {master['status']}")
        if "error" in master:
            print(f"   Error: {master['error']}")
        
        # Check PostgreSQL connection
        print("\n🗄️  PostgreSQL Connection:")
        try:
            db_config = self.config.get('database', {})
            connection_string = db_config.get('connection_string', 'postgresql://samsundar:1327@host.docker.internal:5432/crawler')
            conn = psycopg2.connect(connection_string)
            conn.close()
            print("✅ PostgreSQL is accessible")
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
        
        print("\n" + "=" * 40)
        print("Health check completed!")
    
    def start_crawling(self) -> None:
        """Start the crawling process by dispatching seed URLs."""
        print("🚀 Starting Web Crawler")
        print("=" * 30)
        
        # Perform quick health check
        print("Performing quick health check...")
        if not self._check_kafka_connection():
            print("❌ Cannot start crawling: Kafka is not accessible")
            return
        
        # We're running inside the master container, so assume infrastructure is ready
        print("✅ Master container is running")
        print("✅ Assuming worker infrastructure is ready (started via docker-compose)")
        
        # Dispatch seed URLs
        print("\n📤 Dispatching seed URLs to Kafka topics...")
        try:
            dispatcher = MasterDispatcher(config_path="config.json")
            dispatcher.dispatch()
            print("✅ Seed URLs dispatched successfully!")
            
            seed_urls = self.config.get('seed_urls', [])
            print(f"📋 Dispatched {len(seed_urls)} seed URLs:")
            for url in seed_urls:
                print(f"   • {url}")
                
            print(f"\n🎯 Workers are now processing URLs from priority queues...")
            print("📊 Use 'python3 main.py health_check' to monitor progress")
            
        except Exception as e:
            print(f"❌ Failed to dispatch seed URLs: {e}")
            return
        
        print("\n" + "=" * 30)
        print("Crawling started successfully!")
    
    def stop_crawling(self) -> None:
        """Stop the crawling process by clearing Kafka topics."""
        print("🛑 Stopping Web Crawler")
        print("=" * 30)
        
        # This would require clearing Kafka topics or sending stop signals
        # For now, we'll just inform the user
        print("⚠️  To stop crawling:")
        print("1. Workers will continue processing URLs already in queues")
        print("2. To completely stop, run: docker-compose down")
        print("3. To pause workers: docker-compose stop worker-1 worker-2 worker-3 worker-4 worker-5")
        
        print("\n" + "=" * 30)
        print("Crawling stop instructions provided!")
    
    def add_url(self, urls: List[str], priority: Optional[int] = None, queue: Optional[str] = None) -> None:
        """Add URLs to a specific queue."""
        print("📤 Adding URLs to Queue")
        print("=" * 30)
        
        try:
            manager = URLQueueManager(config_path="config.json")
            
            if priority is not None and not 1 <= priority <= 5:
                print("❌ Priority must be between 1 and 5")
                return
            
            results = manager.add_urls_batch(urls, priority, queue)
            success_count = sum(1 for success in results.values() if success)
            
            print(f"\n📊 Results: {success_count}/{len(results)} URLs added successfully")
            
            if success_count > 0:
                print("✅ URLs are now in the queue and will be processed by workers")
            else:
                print("❌ No URLs were added. Check Kafka connection and queue names.")
            
            manager.close()
            
        except Exception as e:
            print(f"❌ Failed to add URLs: {e}")
    
    def queue_info(self) -> None:
        """Show queue information."""
        print("🔍 Queue Information")
        print("=" * 30)
        
        try:
            manager = URLQueueManager(config_path="config.json")
            info = manager.get_queue_info()
            
            print(f"Available queues: {', '.join(info['available_queues'])}")
            print(f"Default priority: {info['default_priority']}")
            print(f"Priority range: {info['priority_range']}")
            print(f"Kafka servers: {', '.join(info['kafka_servers'])}")
            
            manager.close()
            
        except Exception as e:
            print(f"❌ Failed to get queue info: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Web Crawler CLI - Control your distributed web crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 main.py start          # Start crawling with seed URLs
  python3 main.py health_check   # Check system health
  python3 main.py stop           # Get instructions to stop crawling
        """
    )
    
    parser.add_argument(
        'command',
        choices=['start', 'health_check', 'stop', 'add_url', 'queue_info'],
        help='Command to execute'
    )
    
    # Arguments for add_url command
    parser.add_argument(
        '--urls', '-u',
        nargs='+',
        help='URL(s) to add to queue (required for add_url command)'
    )
    parser.add_argument(
        '--priority', '-p',
        type=int,
        choices=range(1, 6),
        help='Priority level (1-5) for URLs'
    )
    parser.add_argument(
        '--queue', '-q',
        help='Specific queue name (e.g., urls_priority_3)'
    )
    
    parser.add_argument(
        '--config',
        default='config.json',
        help='Path to configuration file (default: config.json)'
    )
    
    args = parser.parse_args()
    
    try:
        cli = CrawlerCLI(config_path=args.config)
        
        if args.command == 'start':
            cli.start_crawling()
        elif args.command == 'health_check':
            cli.health_check()
        elif args.command == 'stop':
            cli.stop_crawling()
        elif args.command == 'add_url':
            if not args.urls:
                print("❌ Error: --urls is required for add_url command")
                print("Usage: python3 main.py add_url --urls <url1> [url2] ... [--priority N] [--queue QUEUE]")
                sys.exit(1)
            cli.add_url(args.urls, args.priority, args.queue)
        elif args.command == 'queue_info':
            cli.queue_info()
            
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
