"""
Robots.txt Checker - Validates URLs against robots.txt rules
"""

import logging
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from typing import Dict, Optional
import time
import httpx


class RobotsChecker:
    """Check if URLs are allowed by robots.txt"""
    
    def __init__(self, user_agent: str = "WebCrawler/1.0"):
        self.user_agent = user_agent
        self.parsers: Dict[str, RobotFileParser] = {}
        self.last_fetch: Dict[str, float] = {}
        self.cache_duration = 3600  # Cache robots.txt for 1 hour
        
        self.logger = logging.getLogger(__name__)
        self.client = httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent
            }
        )
    
    def _get_robots_url(self, url: str) -> str:
        """Get robots.txt URL for a given URL"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    def _get_domain_key(self, url: str) -> str:
        """Get domain key for caching"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def _fetch_robots_txt(self, robots_url: str) -> Optional[RobotFileParser]:
        """Fetch and parse robots.txt"""
        try:
            self.logger.info(f"Fetching robots.txt from {robots_url}")
            
            response = self.client.get(robots_url)
            
            parser = RobotFileParser()
            parser.set_url(robots_url)
            
            if response.status_code == 200:
                # Parse the robots.txt content
                parser.parse(response.text.splitlines())
                self.logger.info(f"Successfully parsed robots.txt from {robots_url}")
            elif response.status_code == 404:
                # No robots.txt means everything is allowed
                self.logger.info(f"No robots.txt found at {robots_url} - allowing all")
                # Empty parser allows everything
                parser.parse([])
            else:
                self.logger.warning(f"Failed to fetch robots.txt: HTTP {response.status_code}")
                # On error, be conservative and allow
                parser.parse([])
            
            return parser
            
        except Exception as e:
            self.logger.error(f"Error fetching robots.txt from {robots_url}: {e}")
            # On error, create permissive parser
            parser = RobotFileParser()
            parser.set_url(robots_url)
            parser.parse([])
            return parser
    
    def _get_parser(self, url: str) -> Optional[RobotFileParser]:
        """Get cached or fetch new robots.txt parser"""
        domain_key = self._get_domain_key(url)
        current_time = time.time()
        
        # Check if we have a cached parser that's still valid
        if domain_key in self.parsers:
            last_fetch = self.last_fetch.get(domain_key, 0)
            if current_time - last_fetch < self.cache_duration:
                return self.parsers[domain_key]
        
        # Fetch new robots.txt
        robots_url = self._get_robots_url(url)
        parser = self._fetch_robots_txt(robots_url)
        
        if parser:
            self.parsers[domain_key] = parser
            self.last_fetch[domain_key] = current_time
        
        return parser
    
    def can_fetch(self, url: str) -> bool:
        """
        Check if URL can be fetched according to robots.txt
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if allowed, False if disallowed
        """
        try:
            parser = self._get_parser(url)
            
            if not parser:
                # If we can't get parser, be conservative and allow
                self.logger.warning(f"No parser available for {url}, allowing by default")
                return True
            
            allowed = parser.can_fetch(self.user_agent, url)
            
            if not allowed:
                self.logger.info(f"URL disallowed by robots.txt: {url}")
            
            return allowed
            
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {e}")
            # On error, be conservative and allow
            return True
    
    def get_crawl_delay(self, url: str) -> Optional[float]:
        """
        Get crawl delay from robots.txt if specified
        
        Args:
            url: URL to check
            
        Returns:
            float: Crawl delay in seconds, or None if not specified
        """
        try:
            parser = self._get_parser(url)
            
            if not parser:
                return None
            
            delay = parser.crawl_delay(self.user_agent)
            
            if delay:
                self.logger.info(f"Crawl delay for {url}: {delay}s")
            
            return delay
            
        except Exception as e:
            self.logger.error(f"Error getting crawl delay for {url}: {e}")
            return None
    
    def close(self):
        """Close HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

