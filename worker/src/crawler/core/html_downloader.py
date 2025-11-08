"""
HTML Downloader - Browser-based approach using Playwright
Uses real browser automation to avoid detection
"""

import logging
import random
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import ipaddress
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


class HTMLDownloader:
    """Downloads HTML content using Playwright browser automation"""

    def __init__(self, timeout: int = 60, max_retries: int = 3,
                 delay: float = 2.0, min_request_interval: float = 5.0,
                 headless: bool = True):
        self.timeout = timeout * 1000  # Convert to milliseconds for Playwright
        self.max_retries = max_retries
        self.delay = delay
        self.min_request_interval = min_request_interval
        self.last_request_time = 0
        self.headless = headless

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Initialize Playwright
        self.playwright = None
        self.browser = None
        self.context = None
        self._init_browser()

    def _init_browser(self):
        """Initialize Playwright browser"""
        try:
            if self.playwright is None:
                self.playwright = sync_playwright().start()

            # Close existing browser if any
            if self.browser:
                try:
                    self.browser.close()
                except BaseException:
                    pass

            # Launch browser with stealth settings
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process'
                ]
            )

            # Create context with realistic settings
            self.context = self.browser.new_context(
                viewport={
                    'width': 1920,
                    'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York',
                permissions=['geolocation'],
                color_scheme='light',
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Upgrade-Insecure-Requests': '1'})

            # Add stealth scripts to avoid detection
            self.context.add_init_script("""
                // Overwrite the `plugins` property to use a custom getter
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Overwrite the `plugins` property to use a custom getter
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Overwrite the `languages` property to use a custom getter
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });

                // Pass the Chrome Test
                window.chrome = {
                    runtime: {}
                };

                // Pass the Permissions Test
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)

            self.logger.info(
                f"Initialized Playwright browser (headless={self.headless})")

        except Exception as e:
            self.logger.error(f"Failed to initialize browser: {e}")
            raise

    def _rate_limit(self):
        """Rate limiting between requests with jitter"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            # Add jitter to avoid pattern detection
            sleep_time += random.uniform(0.5, 2.0)
            self.logger.info(f"Rate limiting: waiting {sleep_time:.2f}s")
            time.sleep(sleep_time)

        self.last_request_time = time.time()

    def download(self, url: str) -> Optional[Dict[str, Any]]:
        """Download HTML content using Playwright browser"""

        if not self.is_valid_url(url):
            self.logger.error(f"Invalid URL: {url}")
            return None

        self._rate_limit()

        for attempt in range(self.max_retries + 1):
            page = None
            try:
                self.logger.info(
                    f"Downloading {url} (attempt {attempt + 1}/{self.max_retries + 1})")

                # Create new page
                page = self.context.new_page()

                # Set longer timeout for navigation
                page.set_default_timeout(self.timeout)
                page.set_default_navigation_timeout(self.timeout)

                # Navigate to URL
                self.logger.info(f"Navigating to {url}...")
                response = page.goto(url, wait_until='networkidle')

                if response is None:
                    self.logger.error("No response received")
                    if attempt < self.max_retries:
                        self._wait_before_retry(attempt)
                        continue
                    return None

                status_code = response.status
                self.logger.info(f"Received response: status={status_code}")

                # Check for blocking status codes
                if status_code == 403:
                    self.logger.warning(
                        f"403 Forbidden - possible block (attempt {attempt + 1})")
                    if attempt >= self.max_retries:
                        return None
                    time.sleep(5)
                    continue

                if status_code == 429:
                    self.logger.warning("429 Too Many Requests - rate limited")
                    if attempt >= self.max_retries:
                        return None
                    time.sleep(10)
                    continue

                if status_code not in [200, 201]:
                    self.logger.warning(
                        f"Unexpected status code: {status_code}")
                    if attempt >= self.max_retries:
                        return None
                    time.sleep(2)
                    continue

                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                    page.wait_for_timeout(2000)
                except BaseException:
                    pass

                # Get page content
                html_content = page.content()

                if len(html_content) < 5000:
                    self.logger.warning(f"Short content: {len(html_content)}")
                    if attempt >= self.max_retries:
                        return None
                    time.sleep(2)
                    continue

                # Check for blocking indicators
                if self._is_blocked(html_content[:10000]):
                    self.logger.warning("Blocking detected in content")
                    if attempt >= self.max_retries:
                        return None
                    time.sleep(3)
                    continue

                self.logger.info(
                    f"Successfully downloaded {len(html_content)} bytes")

                return {
                    'content': html_content,
                    'status_code': status_code,
                    'url': page.url,
                    'content_length': len(html_content),
                    'content_type': 'text/html',
                    'headers': {}
                }

            except PlaywrightTimeout as e:
                self.logger.error(f"Timeout for {url}: {e}")
                if attempt < self.max_retries:
                    self._wait_before_retry(attempt)
                    continue
                return None

            except Exception as e:
                self.logger.error(f"Error downloading {url}: {e}")
                if attempt < self.max_retries:
                    self._wait_before_retry(attempt)
                    continue
                return None

            finally:
                # Always close the page
                if page:
                    try:
                        page.close()
                    except BaseException:
                        pass

        return None

    def _is_blocked(self, html_content: str) -> bool:
        """Check if page shows blocking indicators"""
        block_indicators = [
            'access denied',
            'blocked',
            'captcha',
            'security check',
            'unusual traffic',
            'not available',
            'forbidden',
            'bot detected',
            'automated access',
            'verify you are human',
            'cloudflare',
            'please verify',
        ]

        html_lower = html_content.lower()
        for indicator in block_indicators:
            if indicator in html_lower:
                self.logger.warning(f"Found blocking indicator: '{indicator}'")
                return True

        return False

    def _wait_before_retry(self, attempt: int) -> None:
        """Exponential backoff with jitter"""
        base_wait = self.delay * (2 ** attempt)
        jitter = random.uniform(0.5, 1.5)
        wait_time = base_wait * jitter

        # Cap maximum wait time
        wait_time = min(wait_time, 60)

        self.logger.info(f"Waiting {wait_time:.2f}s before retry")
        time.sleep(wait_time)

    def is_valid_url(self, url: str) -> bool:
        """Validate URL"""
        try:
            if len(url) > 2048:
                return False

            parsed = urlparse(url)
            if not all([parsed.scheme, parsed.netloc]):
                return False

            if parsed.scheme not in ['http', 'https']:
                return False

            if not parsed.netloc or '..' in parsed.netloc:
                return False

            if any(pattern in url.lower()
                   for pattern in ['javascript:', 'data:', 'file:']):
                return False

            if self._is_private_ip(parsed.netloc):
                return False

            return True
        except Exception as e:
            self.logger.error(f"URL validation error: {e}")
            return False

    def _is_private_ip(self, netloc: str) -> bool:
        """Check if netloc is private IP"""
        hostname = netloc.split(':')[0]

        try:
            ip = ipaddress.ip_address(hostname)
            return ip.is_private or ip.is_loopback or ip.is_reserved
        except ValueError:
            return False

    def close(self):
        """Cleanup browser resources"""
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            self.logger.info("Closed browser and cleaned up resources")
        except Exception as e:
            self.logger.error(f"Error closing browser: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        """Cleanup on deletion"""
        try:
            self.close()
        except BaseException:
            pass
