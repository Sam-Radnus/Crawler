import re
from urllib.parse import urlparse
from typing import List


class PropertyURLMatcher:
    """Craigslist property URL matcher with strict filtering."""

    def __init__(self):
        # Craigslist subdomains (e.g. newyork.craigslist.org)
        self.craigslist_domain_pattern = re.compile(
            r'^[a-z0-9\-]+\.craigslist\.org$',
            re.IGNORECASE
        )

        # Listing page: https://city.craigslist.org/search/apa#...
        self.listing_pattern = re.compile(
            r'^https?://[a-z0-9\-]+\.craigslist\.org(?:/search/apa)(?:[#?].*)?$',
            re.IGNORECASE)

        # Property detail page:
        # https://city.craigslist.org/apa/d/title/1234567890.html
        self.property_pattern = re.compile(
            r'^https?://[a-z0-9\-]+\.craigslist\.org/apa/d/[^/]+/\d+\.html$',
            re.IGNORECASE
        )

    def is_allowed_domain(self, url: str) -> bool:
        """Return True if domain is a Craigslist subdomain."""
        try:
            parsed = urlparse(url)
            return bool(
                self.craigslist_domain_pattern.match(
                    parsed.netloc.lower()))
        except Exception:
            return False

    def is_listing_url(self, url: str) -> bool:
        """Return True if URL is a Craigslist apartment listing search page."""
        return self.is_allowed_domain(url) and bool(
            self.listing_pattern.match(url))

    def is_property_url(self, url: str) -> bool:
        # Simple check: craigslist.org + apa + .html in correct order
        try:
            url_lower = url.lower()
            return (
                'craigslist.org' in url_lower and
                'apa' in url_lower and
                url_lower.endswith('.html') and
                url_lower.find('craigslist.org') < url_lower.find('apa') < url_lower.find('.html')
            )
        except Exception:
            return False

    def is_relevant_url(self, url: str) -> bool:
        """Return True if URL is either listing or property page."""
        return self.is_listing_url(url) or self.is_property_url(url)

    def filter_property_urls(self, urls: List[str]) -> List[str]:
        """Filter list to only property or listing URLs."""
        return [url for url in urls if self.is_relevant_url(url)]


# Standalone function
def is_property_url(url: str) -> bool:
    """Simple check for Craigslist property URLs."""
    try:
        url_lower = url.lower()
        return (
            'craigslist.org' in url_lower and
            'apa' in url_lower and
            url_lower.endswith('.html') and
            url_lower.find('craigslist.org') < url_lower.find('apa') < url_lower.find('.html')
        )
    except Exception:
        return False
