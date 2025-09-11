"""
Web Crawler - Main orchestrator for the web crawling system
"""
import json
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from url_frontier import URLFrontier
from html_downloader import HTMLDownloader
from robots_parser import RobotsParser
from link_extractor import LinkExtractor
from url_seen import URLSeen
from content_storage import ContentStorage
from logger import CrawlerLogger


class WebCrawler:
    """Main web crawler orchestrator"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize crawler with configuration"""
        self.config = self._load_config(config_path)
        self.logger = CrawlerLogger(
            log_level="INFO",
            stats_interval=self.config.get('stats_interval', 10)
        )
        self.max_workers = self.config.get('max_workers', 3)
        
        # Initialize components
        self.url_frontier = URLFrontier(
            max_size=self.config.get('max_queue_size', 1000)
        )
        self.html_downloader = HTMLDownloader(
            timeout=self.config.get('timeout', 10),
            max_retries=self.config.get('max_retries', 3),
            delay=self.config.get('delay_between_requests', 1.0),
            user_agent=self.config.get('user_agent', 'WebCrawler/1.0')
        )
        self.robots_parser = RobotsParser(
            user_agent=self.config.get('user_agent', 'WebCrawler/1.0')
        )
        self.link_extractor = LinkExtractor()
        self.url_seen = URLSeen(capacity=100000)
        self.content_storage = ContentStorage(
            db_path=self.config.get('db_path', 'crawler.db'),
            output_dir=self.config.get('output_dir', 'crawled_data')
        )
        
        # Crawling state
        self.is_crawling = False
        self.max_pages = self.config.get('max_pages', 100)
        self.respect_robots = self.config.get('respect_robots', True)
        
        self.logger.log_info("Web crawler initialized successfully")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            self.logger.log_error(f"Configuration file {config_path} not found")
            raise
        except json.JSONDecodeError as e:
            self.logger.log_error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def start_crawling(self):
        """Start the crawling process"""
        if self.is_crawling:
            self.logger.log_warning("Crawler is already running")
            return
        
        self.is_crawling = True
        self.logger.log_info("Starting web crawler...")
        
        # Add seed URLs to frontier
        self._add_seed_urls()
        
        # Main crawling loop
        try:
            self._crawl_loop()
        except KeyboardInterrupt:
            self.logger.log_info("Crawling interrupted by user")
        except Exception as e:
            self.logger.log_error(f"Unexpected error during crawling: {e}")
        finally:
            self.is_crawling = False
            self._print_final_stats()
    
    def _add_seed_urls(self):
        """Add seed URLs to the frontier"""
        seed_urls = self.config.get('seed_urls', [])
        
        for url in seed_urls:
            if self.html_downloader.is_valid_url(url):
                # Check robots.txt for seed URLs
                if self.respect_robots:
                    self.robots_parser.update_robots_for_domain(url)
                    if not self.robots_parser.is_allowed(url):
                        self.logger.log_warning(f"Seed URL blocked by robots.txt: {url}")
                        self.logger.increment_robots_blocked()
                        continue
                
                # Add to frontier and mark as seen
                if self.url_frontier.add_url(url):
                    self.url_seen.add_url(url)
                    self.logger.log_info(f"Added seed URL: {url}")
                else:
                    self.logger.log_warning(f"Failed to add seed URL (queue full): {url}")
            else:
                self.logger.log_warning(f"Invalid seed URL: {url}")
    
    def _crawl_loop(self):
        """Main crawling loop with producer–consumer pattern"""
        pages_crawled = 0
        futures = {}
    
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Prime the executor with initial tasks (up to max_workers)
            for _ in range(min(self.max_workers, self.url_frontier.size())):
                url = self.url_frontier.get_url()
                if not url:
                    break
                self.logger.update_queue_size(self.url_frontier.size())
                # Schedule _crawl_url(url) to run in a thread, get back a Future, and store a mapping from that future back to the original url.
                futures[executor.submit(self._crawl_url, url)] = url
    
            # Process tasks as they finish
            while futures and self.is_crawling and pages_crawled < self.max_pages:
                for future in as_completed(futures):
                    url = futures.pop(future)
    
                    try:
                        success = future.result()
                    except Exception as e:
                        self.logger.log_error(f"Error in future for {url}: {e}")
                        success = False
    
                    # Update stats
                    pages_crawled += 1
                    self.logger.increment_pages_crawled()
                    if success:
                        self.logger.increment_pages_successful()
                    else:
                        self.logger.increment_pages_failed()
    
                    # Respect crawl delay (per-domain)
                    if self.respect_robots:
                        delay = self.robots_parser.get_crawl_delay(url)
                        if delay > 0:
                            time.sleep(delay)
                    else:
                        time.sleep(self.config.get('delay_between_requests', 1.0))
    
                    # Schedule a new task if URLs remain
                    if (self.is_crawling and 
                        pages_crawled < self.max_pages and 
                        not self.url_frontier.is_empty()):
                        next_url = self.url_frontier.get_url()
                        if next_url:
                            self.logger.update_queue_size(self.url_frontier.size())
                            futures[executor.submit(self._crawl_url, next_url)] = next_url
    
        self.logger.log_info(f"Crawling completed. Total pages crawled: {pages_crawled}")

    
    def _crawl_url(self, url: str) -> bool:
        """Crawl a single URL"""
        start_time = time.time()
        
        try:
            self.logger.log_info(f"Crawling: {url}")
            
            # Check robots.txt compliance
            if self.respect_robots and not self.robots_parser.is_allowed(url):
                self.logger.log_warning(f"URL blocked by robots.txt: {url}")
                self.logger.increment_robots_blocked()
                return False
            
            # Download HTML content
            result = self.html_downloader.download(url)
            if not result:
                self.logger.log_error(f"Failed to download: {url}")
                return False
            
            # Save content to storage
            crawl_duration = time.time() - start_time
            success = self.content_storage.save_page(
                url=url,
                html_content=result['content'],
                status_code=result['status_code'],
                headers=result['headers'],
                crawl_duration=crawl_duration
            )
            
            if not success:
                self.logger.log_error(f"Failed to save content for: {url}")
                return False
            
            # Extract and process links
            self._process_links(url, result['content'])
            
            return True
            
        except Exception as e:
            self.logger.log_error(f"Error crawling {url}: {e}")
            return False
    
    def _process_links(self, base_url: str, html_content: str):
        """Extract and process links from HTML content"""
        try:
            # Extract links
            links = self.link_extractor.extract_links(html_content, base_url)
            
            if not links:
                return
            
            # Update links found counter
            self.logger.increment_links_found(len(links))
            
            # Add new links to frontier
            new_links_added = 0
            for link in links:
                # Check if we've seen this URL before
                if not self.url_seen.has_seen(link):
                    # Check robots.txt compliance
                    if self.respect_robots and not self.robots_parser.is_allowed(link):
                        self.logger.increment_robots_blocked()
                        continue
                    
                    # Add to frontier
                    if self.url_frontier.add_url(link):
                        self.url_seen.add_url(link)
                        new_links_added += 1
                    else:
                        self.logger.log_warning(f"Queue full, cannot add: {link}")
                        break
            
            self.logger.log_debug(f"Added {new_links_added} new links from {base_url}")
            
        except Exception as e:
            self.logger.log_error(f"Error processing links from {base_url}: {e}")
    
    def _print_final_stats(self):
        """Print final crawling statistics"""
        self.logger.log_info("=" * 60)
        self.logger.log_info("FINAL CRAWLING STATISTICS")
        self.logger.log_info("=" * 60)
        
        # Get stats from logger
        stats = self.logger.get_stats()
        for key, value in stats.items():
            self.logger.log_info(f"{key}: {value}")
        
        # Get stats from storage
        storage_stats = self.content_storage.get_stats()
        if storage_stats:
            self.logger.log_info("Storage Statistics:")
            for key, value in storage_stats.items():
                self.logger.log_info(f"  {key}: {value}")
        
        self.logger.log_info("=" * 60)
    
    def stop_crawling(self):
        """Stop the crawling process"""
        self.is_crawling = False
        self.logger.log_info("Stopping crawler...")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current crawler status"""
        return {
            'is_crawling': self.is_crawling,
            'queue_size': self.url_frontier.size(),
            'pages_crawled': self.logger.get_stats()['pages_crawled'],
            'urls_seen': self.url_seen.size(),
            'max_pages': self.max_pages
        }


def main():
    """Main entry point"""
    try:
        crawler = WebCrawler()
        crawler.start_crawling()
    except Exception as e:
        print(f"Error starting crawler: {e}")


if __name__ == "__main__":
    main()
