#!/usr/bin/env python3
"""
Example usage of the web crawler
"""
import json
import time
from web_crawler import WebCrawler

def create_example_config():
    """Create an example configuration file"""
    config = {
        "seed_urls": [
            "https://httpbin.org",
            "https://example.com"
        ],
        "max_pages": 5,
        "delay_between_requests": 2.0,
        "timeout": 10,
        "max_retries": 2,
        "user_agent": "WebCrawler/1.0",
        "respect_robots": True,
        "max_queue_size": 100,
        "stats_interval": 5,
        "output_dir": "example_output",
        "db_path": "example_crawler.db"
    }
    
    with open("example_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    print("Created example_config.json")

def run_example_crawler():
    """Run the crawler with example configuration"""
    try:
        # Create example config
        create_example_config()
        
        print("Starting example web crawler...")
        print("This will crawl a few pages and demonstrate the features.")
        print("Press Ctrl+C to stop early.\n")
        
        # Initialize and start crawler
        crawler = WebCrawler("example_config.json")
        
        # Start crawling in a separate thread to allow for interruption
        import threading
        crawler_thread = threading.Thread(target=crawler.start_crawling)
        crawler_thread.daemon = True
        crawler_thread.start()
        
        # Monitor progress
        while crawler_thread.is_alive():
            status = crawler.get_status()
            print(f"Status: {status['pages_crawled']} pages crawled, "
                  f"{status['queue_size']} URLs in queue")
            time.sleep(2)
        
        print("\nCrawling completed!")
        
        # Show final results
        storage_stats = crawler.content_storage.get_stats()
        print(f"\nFinal Statistics:")
        print(f"Total pages: {storage_stats.get('total_pages', 0)}")
        print(f"Successful: {storage_stats.get('successful_pages', 0)}")
        print(f"Failed: {storage_stats.get('failed_pages', 0)}")
        
        # Show recent pages
        recent_pages = crawler.content_storage.get_recent_pages(5)
        if recent_pages:
            print(f"\nRecent pages crawled:")
            for page in recent_pages:
                print(f"  - {page['url']} (Status: {page['status_code']})")
        
    except KeyboardInterrupt:
        print("\nCrawling interrupted by user")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_example_crawler()
