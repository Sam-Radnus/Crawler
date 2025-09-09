"""
HTML Downloader - Downloads web pages with retries and timeout
"""
import requests
import time
import logging
from typing import Optional, Dict, Any
from urllib.parse import urlparse


class HTMLDownloader:
    """Downloads HTML content with retry logic and timeout"""
    
    def __init__(self, timeout: int = 10, max_retries: int = 3, 
                 delay: float = 1.0, user_agent: str = "WebCrawler/1.0"):
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay = delay
        self.user_agent = user_agent
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
        
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def download(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Download HTML content from URL with retry logic
        
        Returns:
            Dict with 'content', 'status_code', 'headers', 'url' or None if failed
        """
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"Downloading {url} (attempt {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                # Check if response is successful
                if response.status_code == 200:
                    return {
                        'content': response.text,
                        'status_code': response.status_code,
                        'headers': dict(response.headers),
                        'url': response.url,
                        'content_length': len(response.content)
                    }
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    if attempt < self.max_retries:
                        time.sleep(self.delay * (2 ** attempt))  # Exponential backoff
                        continue
                    else:
                        return None
                        
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {url}: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.delay * (2 ** attempt))
                    continue
                else:
                    return None
            except Exception as e:
                self.logger.error(f"Unexpected error downloading {url}: {e}")
                return None
        
        return None
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and accessible"""
        try:
            parsed = urlparse(url)
            return bool(parsed.scheme and parsed.netloc)
        except Exception:
            return False
    
    def get_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except Exception:
            return None
