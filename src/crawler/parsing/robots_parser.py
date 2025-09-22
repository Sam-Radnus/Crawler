"""
Robots.txt compliance parser
"""
import requests
import logging
from urllib.parse import urljoin, urlparse
from typing import Set, List, Optional
import re


class RobotsParser:
    """Parse and enforce robots.txt rules"""
    
    def __init__(self, user_agent: str = "WebCrawler/1.0"):
        self.user_agent = user_agent
        self.crawler_delays = {}  # Domain -> delay in seconds
        self.disallowed_paths = {}  # Domain -> set of disallowed paths
        self.logger = logging.getLogger(__name__)
    
    def fetch_robots_txt(self, base_url: str) -> Optional[str]:
        """Fetch robots.txt content for a domain"""
        try:
            parsed = urlparse(base_url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            
            response = requests.get(robots_url, timeout=5)
            if response.status_code == 200:
                return response.text
            else:
                self.logger.info(f"No robots.txt found for {parsed.netloc}")
                return None
        except Exception as e:
            self.logger.warning(f"Failed to fetch robots.txt for {base_url}: {e}")
            return None
    
    def parse_robots_txt(self, robots_content: str, base_url: str) -> None:
        """Parse robots.txt content and store rules"""
        domain = urlparse(base_url).netloc
        
        if domain not in self.disallowed_paths:
            self.disallowed_paths[domain] = set()
            self.crawler_delays[domain] = 1.0  # Default delay
        
        current_user_agent = None
        lines = robots_content.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Parse User-agent
            if line.lower().startswith('user-agent:'):
                current_user_agent = line[11:].strip()
                continue
            
            # Parse Disallow rules
            if line.lower().startswith('disallow:') and current_user_agent:
                if (current_user_agent == '*' or 
                    current_user_agent.lower() in self.user_agent.lower()):
                    path = line[9:].strip()
                    if path:
                        self.disallowed_paths[domain].add(path)
            
            # Parse Crawl-delay
            elif line.lower().startswith('crawl-delay:') and current_user_agent:
                if (current_user_agent == '*' or 
                    current_user_agent.lower() in self.user_agent.lower()):
                    try:
                        delay = float(line[12:].strip())
                        self.crawler_delays[domain] = delay
                    except ValueError:
                        pass
    
    def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            path = parsed.path
            
            if domain not in self.disallowed_paths:
                return True
            
            # Check if any disallowed path matches
            for disallowed_path in self.disallowed_paths[domain]:
                if self._path_matches(path, disallowed_path):
                    return False
            
            return True
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {e}")
            return True  # Default to allowed if error
    
    def _path_matches(self, url_path: str, disallowed_path: str) -> bool:
        """Check if URL path matches disallowed pattern"""
        if not disallowed_path:
            return False
        
        # Handle wildcard patterns
        if '*' in disallowed_path:
            pattern = disallowed_path.replace('*', '.*')
            return bool(re.match(pattern, url_path))
        
        # Exact match or prefix match
        return url_path.startswith(disallowed_path)
    
    def get_crawl_delay(self, url: str) -> float:
        """Get recommended crawl delay for domain"""
        try:
            domain = urlparse(url).netloc
            return self.crawler_delays.get(domain, 1.0)
        except Exception:
            return 1.0
    
    def update_robots_for_domain(self, base_url: str) -> None:
        """Fetch and parse robots.txt for a domain"""
        robots_content = self.fetch_robots_txt(base_url)
        if robots_content:
            self.parse_robots_txt(robots_content, base_url)
            domain = urlparse(base_url).netloc
            self.logger.info(f"Updated robots.txt rules for {domain}")
