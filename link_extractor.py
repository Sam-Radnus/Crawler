"""
Link Extractor - Extract and normalize <a href> links from HTML
"""
import re
import logging
from urllib.parse import urljoin, urlparse, urlunparse
from typing import Set, List, Optional
from bs4 import BeautifulSoup


class LinkExtractor:
    """Extract and normalize links from HTML content"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Common file extensions to skip
        self.skip_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.jpg', '.jpeg', '.png', '.gif',
            '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm'
        }
    
    def extract_links(self, html_content: str, base_url: str) -> Set[str]:
        """
        Extract all <a href> links from HTML content
        
        Args:
            html_content: Raw HTML content
            base_url: Base URL for resolving relative links
            
        Returns:
            Set of normalized absolute URLs
        """
        links = set()
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all <a> tags with href attributes
            for link in soup.find_all('a', href=True):
                href = link['href'].strip()
                if not href:
                    continue
                
                # Convert relative URLs to absolute
                absolute_url = urljoin(base_url, href)
                
                # Normalize the URL
                normalized_url = self._normalize_url(absolute_url)
                
                if normalized_url and self._is_valid_link(normalized_url):
                    links.add(normalized_url)
            
            self.logger.info(f"Extracted {len(links)} links from {base_url}")
            return links
            
        except Exception as e:
            self.logger.error(f"Error extracting links from {base_url}: {e}")
            return set()
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize URL by removing fragments, sorting query parameters, etc.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL or None if invalid
        """
        try:
            parsed = urlparse(url)
            
            # Skip non-HTTP(S) URLs
            if parsed.scheme not in ['http', 'https']:
                return None
            
            # Remove fragment
            normalized = parsed._replace(fragment='')
            
            # Convert to lowercase for hostname
            if normalized.hostname:
                normalized = normalized._replace(netloc=normalized.netloc.lower())
            
            # Remove trailing slash for consistency (except root)
            path = normalized.path
            if path and path != '/' and path.endswith('/'):
                path = path[:-1]
                normalized = normalized._replace(path=path)
            
            return urlunparse(normalized)
            
        except Exception as e:
            self.logger.error(f"Error normalizing URL {url}: {e}")
            return None
    
    def _is_valid_link(self, url: str) -> bool:
        """
        Check if link should be crawled
        
        Args:
            url: URL to validate
            
        Returns:
            True if URL should be crawled
        """
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Skip non-HTTP(S) URLs
            if parsed.scheme not in ['http', 'https']:
                return False
            
            # Skip URLs with file extensions we don't want to crawl
            path = parsed.path.lower()
            for ext in self.skip_extensions:
                if path.endswith(ext):
                    return False
            
            # Skip mailto, tel, javascript, etc.
            if parsed.scheme in ['mailto', 'tel', 'javascript', 'ftp']:
                return False
            
            # Skip URLs that are too long (potential infinite loops)
            if len(url) > 2000:
                return False
            
            return True
            
        except Exception:
            return False
    
    def get_domain_links(self, links: Set[str], target_domain: str) -> Set[str]:
        """
        Filter links to only include those from the same domain
        
        Args:
            links: Set of URLs
            target_domain: Target domain to filter by
            
        Returns:
            Set of URLs from the same domain
        """
        domain_links = set()
        
        for link in links:
            try:
                parsed = urlparse(link)
                if parsed.netloc == target_domain:
                    domain_links.add(link)
            except Exception:
                continue
        
        return domain_links
    
    def get_external_links(self, links: Set[str], current_domain: str) -> Set[str]:
        """
        Get links to external domains
        
        Args:
            links: Set of URLs
            current_domain: Current domain to exclude
            
        Returns:
            Set of external URLs
        """
        external_links = set()
        
        for link in links:
            try:
                parsed = urlparse(link)
                if parsed.netloc and parsed.netloc != current_domain:
                    external_links.add(link)
            except Exception:
                continue
        
        return external_links
