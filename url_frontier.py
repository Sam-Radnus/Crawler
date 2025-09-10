"""
URL Frontier - Simple FIFO queue for managing URLs to crawl
"""
import queue
import threading
from typing import Optional


class URLFrontier:
    """Thread-safe FIFO queue for URL management"""
    
    def __init__(self, max_size: int = 1000):
        self._queue = queue.Queue(maxsize = max_size)
        self._lock = threading.Lock()
        self._size = 0
        self._max_size = max_size
    
    def add_url(self, url: str) -> bool:
        """Add URL to frontier if not full"""
        try:
            with self._lock:
                if self._size < self._max_size:
                    self._queue.put(url, block=False)
                    self._size += 1
                    return True
                return False
        except queue.Full:
            return False
    
    def get_url(self) -> Optional[str]:
        """Get next URL from frontier"""
        try:
            with self._lock:
                url = self._queue.get(block=False)
                self._size -= 1
                return url
        except queue.Empty:
            return None
    
    def is_empty(self) -> bool:
        """Check if frontier is empty"""
        return self._queue.empty()
    
    def size(self) -> int:
        """Get current queue size"""
        return self._size
    
    def is_full(self) -> bool:
        """Check if frontier is full"""
        return self._size >= self._max_size
    
    def clear(self):
        """Clear all URLs from frontier"""
        with self._lock:
            while not self._queue.empty():
                try:
                    self._queue.get(block=False)
                except queue.Empty:
                    break
            self._size = 0
