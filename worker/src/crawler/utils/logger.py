"""
Logging and Metrics - Centralized logging and statistics tracking
"""
import logging
import time
import threading
from typing import Dict, Any
from datetime import datetime


# Module-level thread-local so filters can access across modules
THREAD_LOCAL = threading.local()


class UrlPrefixFilter(logging.Filter):
    """Logging filter that prefixes messages with the current URL if available."""
    def filter(self, record: logging.LogRecord) -> bool:
        current_url = getattr(THREAD_LOCAL, 'current_url', None)
        if current_url:
            # Ensure we prefix the formatted message and reset args to avoid double formatting
            record.msg = f"[{current_url}] {record.getMessage()}"
            record.args = ()
        return True


class CrawlerLogger:
    """Centralized logging and metrics for the web crawler"""
    
    def __init__(self, log_level: str = "INFO", stats_interval: int = 10):
        self.stats_interval = stats_interval
        self.start_time = time.time()
        self.last_stats_time = self.start_time
        
        # Statistics counters
        self.stats = {
            'pages_crawled': 0,
            'pages_successful': 0,
            'pages_failed': 0,
            'links_found': 0,
            'queue_size': 0,
            'errors': 0,
            'robots_blocked': 0
        }
        
        # Thread lock for thread-safe operations
        self.lock = threading.Lock()
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Start stats reporting thread
        self._start_stats_thread()
    
    def _setup_logging(self, log_level: str):
        """Setup logging configuration"""
        # Configure root logger so all module loggers use the same handlers/filters
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.upper()))
        root_logger.handlers.clear()
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, log_level.upper()))
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add URL prefix filter
        url_filter = UrlPrefixFilter()
        console_handler.addFilter(url_filter)
        
        # Add handler to root logger
        root_logger.addHandler(console_handler)
        
        # Keep a named logger for crawler-specific calls
        self.logger = logging.getLogger('web_crawler')
    
    def _start_stats_thread(self):
        """Start background thread for periodic stats reporting"""
        def stats_reporter():
            while True:
                time.sleep(self.stats_interval)
                self._print_stats()
        
        stats_thread = threading.Thread(target=stats_reporter, daemon=True)
        stats_thread.start()
    
    def _print_stats(self):
        """Print current statistics"""
        with self.lock:
            current_time = time.time()
            elapsed_time = current_time - self.start_time
            time_since_last = current_time - self.last_stats_time
            
            # Calculate rates
            pages_per_second = self.stats['pages_crawled'] / elapsed_time if elapsed_time > 0 else 0
            recent_pages_per_second = (self.stats['pages_crawled'] - self.stats.get('last_pages_crawled', 0)) / time_since_last if time_since_last > 0 else 0
            
            self.logger.info("=" * 60)
            self.logger.info("CRAWLER STATISTICS")
            self.logger.info("=" * 60)
            self.logger.info(f"Runtime: {elapsed_time:.1f} seconds")
            self.logger.info(f"Pages crawled: {self.stats['pages_crawled']}")
            self.logger.info(f"Successful: {self.stats['pages_successful']}")
            self.logger.info(f"Failed: {self.stats['pages_failed']}")
            self.logger.info(f"Links found: {self.stats['links_found']}")
            self.logger.info(f"Queue size: {self.stats['queue_size']}")
            self.logger.info(f"Errors: {self.stats['errors']}")
            self.logger.info(f"Robots blocked: {self.stats['robots_blocked']}")
            self.logger.info(f"Overall rate: {pages_per_second:.2f} pages/sec")
            self.logger.info(f"Recent rate: {recent_pages_per_second:.2f} pages/sec")
            
            if self.stats['pages_crawled'] > 0:
                success_rate = (self.stats['pages_successful'] / self.stats['pages_crawled']) * 100
                self.logger.info(f"Success rate: {success_rate:.1f}%")
            
            self.logger.info("=" * 60)
            
            # Update last stats time and pages count
            self.last_stats_time = current_time
            self.stats['last_pages_crawled'] = self.stats['pages_crawled']
    
    def increment_pages_crawled(self):
        """Increment pages crawled counter"""
        with self.lock:
            self.stats['pages_crawled'] += 1
    
    def increment_pages_successful(self):
        """Increment successful pages counter"""
        with self.lock:
            self.stats['pages_successful'] += 1
    
    def increment_pages_failed(self):
        """Increment failed pages counter"""
        with self.lock:
            self.stats['pages_failed'] += 1
    
    def increment_links_found(self, count: int = 1):
        """Increment links found counter"""
        with self.lock:
            self.stats['links_found'] += count
    
    def increment_errors(self):
        """Increment errors counter"""
        with self.lock:
            self.stats['errors'] += 1
    
    def increment_robots_blocked(self):
        """Increment robots blocked counter"""
        with self.lock:
            self.stats['robots_blocked'] += 1
    
    def update_queue_size(self, size: int):
        """Update queue size"""
        with self.lock:
            self.stats['queue_size'] = size
    
    def set_current_url(self, url: str) -> None:
        """Set the current URL context for the calling thread."""
        THREAD_LOCAL.current_url = url
    
    def clear_current_url(self) -> None:
        """Clear the current URL context for the calling thread."""
        THREAD_LOCAL.current_url = None
    
    def log_info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def log_warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def log_error(self, message: str):
        """Log error message"""
        self.logger.error(message)
        self.increment_errors()
    
    def log_debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        with self.lock:
            return self.stats.copy()
    
    def reset_stats(self):
        """Reset all statistics"""
        with self.lock:
            self.stats = {
                'pages_crawled': 0,
                'pages_successful': 0,
                'pages_failed': 0,
                'links_found': 0,
                'queue_size': 0,
                'errors': 0,
                'robots_blocked': 0
            }
            self.start_time = time.time()
            self.last_stats_time = self.start_time
