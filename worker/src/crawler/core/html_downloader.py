"""
HTML Downloader - Downloads web pages with retries and timeout
"""
import requests
import time
import logging
import ipaddress
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
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def download(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Download HTML content from URL with retry logic
        
        Returns:
            Dict with 'content', 'status_code', 'headers', 'url' or None if failed
        """
        # Validate URL before attempting download
        if not self.is_valid_url(url):
            self.logger.error(f"Invalid URL: {url}")
            return None
            
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"Downloading {url} (attempt {attempt + 1})")
                
                response = self.session.get(
                    url, 
                    timeout=self.timeout,
                    allow_redirects=True
                )
                
                # Check if response is successful (2xx status codes)
                if 200 <= response.status_code < 300:
                    # Validate content type
                    content_type = response.headers.get('content-type', '').lower()
                    if 'text/html' not in content_type and 'text/plain' not in content_type:
                        self.logger.warning(f"Non-HTML content type for {url}: {content_type}")
                    
                    return {
                        'content': response.text,
                        'status_code': response.status_code,
                        'headers': dict(response.headers),
                        'url': response.url,
                        'content_length': len(response.content),
                        'content_type': content_type
                    }
                else:
                    self.logger.warning(f"HTTP {response.status_code} for {url}")
                    if attempt < self.max_retries:
                        self._wait_before_retry(attempt)
                        continue
                    else:
                        return None
                        
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {url}: {e}")
                if attempt < self.max_retries:
                    self._wait_before_retry(attempt)
                    continue
                else:
                    return None
            except Exception as e:
                self.logger.error(f"Unexpected error downloading {url}: {e}")
                return None
        
        return None
    
    def _wait_before_retry(self, attempt: int) -> None:
        """Wait before retry with exponential backoff"""
        wait_time = self.delay * (2 ** attempt)
        self.logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
        time.sleep(wait_time)
    
    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and accessible"""
        
        try:
            # Basic URL length check
            if len(url) > 2048:  # RFC 7230 recommends 8000, but 2048 is safer
                return False
                
            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False
            
            if parsed.scheme not in ['http', 'https']:
                return False
            
            if not parsed.netloc or '..' in parsed.netloc:
                return False
            
            # Check for suspicious patterns
            if any(pattern in url.lower() for pattern in ['javascript:', 'data:', 'file:']):
                return False
            
            if self._is_private_ip(parsed.netloc):
                return False
                        
            return True
        except Exception as e:
            self.logger.error(f"Error checking if URL is valid: {e}")
            return False
     
    def _is_private_ip(self, netloc: str) -> bool:
        """Check if netloc is private/local IP"""
        
        # Extract hostname without port
        hostname = netloc.split(':')[0]
        
        try:
            ip = ipaddress.ip_address(hostname)
            return ip.is_private or ip.is_loopback or ip.is_reserved
        except ValueError:
            # Not an IP address, could be a hostname
            return False