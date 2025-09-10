"""
URL Seen - HashSet and Bloom filter for URL deduplication
"""
import logging
from typing import Set
from pybloom_live import BloomFilter


class URLSeen:
    """URL deduplication using HashSet and Bloom filter"""
    
    def __init__(self, capacity: int = 100000, error_rate: float = 0.001):
        """
        Initialize URL seen tracker
        
        Args:
            capacity: Expected number of URLs (for Bloom filter sizing)
            error_rate: False positive rate for Bloom filter
        """
        self.capacity = capacity
        self.error_rate = error_rate
        
        # HashSet for exact matching (no false positives)
        self._url_set: Set[str] = set()
        
        # Bloom filter for quick negative checks
        self._bloom_filter = BloomFilter(capacity=capacity, error_rate=error_rate)
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized URLSeen with capacity {capacity}, error rate {error_rate}")
    
    def add_url(self, url: str) -> bool:
        """
        Add URL to seen set
        
        Args:
            url: URL to add
            
        Returns:
            True if URL was added (not seen before), False if already seen
        """
        try:
            # First check Bloom filter for quick negative
            if url not in self._bloom_filter:
                # Definitely not seen before
                self._bloom_filter.add(url)
                self._url_set.add(url)
                return True
            
            # Check HashSet for exact match
            if url in self._url_set:
                return False  # Already seen
            
            # False positive from Bloom filter, add to both
            self._bloom_filter.add(url)
            self._url_set.add(url)
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding URL {url}: {e}")
            return False
    
    def has_seen(self, url: str) -> bool:
        """
        Check if URL has been seen before
        
        Args:
            url: URL to check
            
        Returns:
            True if URL has been seen, False otherwise
        """
        try:
            # Quick check with Bloom filter first
            if url not in self._bloom_filter:
                return False  # Definitely not seen
            
            # Check HashSet for exact match
            return url in self._url_set
            
        except Exception as e:
            self.logger.error(f"Error checking URL {url}: {e}")
            return False
    
    def size(self) -> int:
        """Get number of URLs seen"""
        return len(self._url_set)
    
    def clear(self):
        """Clear all seen URLs"""
        self._url_set.clear()
        self._bloom_filter = BloomFilter(capacity=self.capacity, error_rate=self.error_rate)
        self.logger.info("Cleared all seen URLs")
    
    def get_stats(self) -> dict:
        """Get statistics about the URL seen tracker"""
        return {
            'total_urls': len(self._url_set),
            'bloom_filter_capacity': self.capacity,
            'bloom_filter_error_rate': self.error_rate,
            'memory_usage_estimate': len(self._url_set) * 100  # Rough estimate in bytes
        }
    
    def is_full(self) -> bool:
        """Check if approaching capacity limits"""
        return len(self._url_set) >= self.capacity * 0.9  # 90% of capacity
    
    def resize(self, new_capacity: int):
        """
        Resize the Bloom filter and HashSet
        
        Args:
            new_capacity: New capacity for the Bloom filter
        """
        if new_capacity <= len(self._url_set):
            self.logger.warning(f"Cannot resize to {new_capacity}, current size is {len(self._url_set)}")
            return
        
        # Create new Bloom filter with new capacity
        old_urls = list(self._url_set)
        self.capacity = new_capacity
        self._bloom_filter = BloomFilter(capacity=new_capacity, error_rate=self.error_rate)
        
        # Re-add all URLs to new Bloom filter
        for url in old_urls:
            self._bloom_filter.add(url)
        
        self.logger.info(f"Resized URLSeen to capacity {new_capacity}")
