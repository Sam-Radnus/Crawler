# Web Crawler

A modular, extensible Python web crawler with comprehensive features for web scraping and data collection.

## Features

- **Configurable Seed URLs** - Load starting URLs from JSON configuration
- **URL Frontier** - Thread-safe FIFO queue for URL management
- **HTML Downloader** - Robust downloading with retries, timeouts, and error handling
- **Robots.txt Compliance** - Respects robots.txt rules and crawl delays
- **Link Extraction** - Extracts and normalizes `<a href>` links from HTML
- **URL Deduplication** - Uses HashSet and Bloom filter for efficient deduplication
- **Content Storage** - SQLite database with comprehensive metadata tracking
- **Logging & Metrics** - Real-time statistics and comprehensive logging
- **Modular Design** - Easy to extend and debug

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd crawler
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to customize crawler behavior:

```json
{
    "seed_urls": [
        "https://example.com",
        "https://httpbin.org"
    ],
    "max_pages": 100,
    "delay_between_requests": 1.0,
    "timeout": 10,
    "max_retries": 3,
    "user_agent": "WebCrawler/1.0",
    "respect_robots": true,
    "max_queue_size": 1000,
    "stats_interval": 10,
    "output_dir": "crawled_data",
    "db_path": "crawler.db"
}
```

### Configuration Options

- `seed_urls`: List of starting URLs to crawl
- `max_pages`: Maximum number of pages to crawl
- `delay_between_requests`: Delay between requests in seconds
- `timeout`: Request timeout in seconds
- `max_retries`: Maximum retry attempts for failed requests
- `user_agent`: User agent string for requests
- `respect_robots`: Whether to respect robots.txt rules
- `max_queue_size`: Maximum size of URL queue
- `stats_interval`: Interval for printing statistics (seconds)
- `output_dir`: Directory to store crawled HTML files
- `db_path`: Path to SQLite database file

## Usage

### Basic Usage

```bash
python web_crawler.py
```

### Programmatic Usage

```python
from web_crawler import WebCrawler

# Initialize crawler
crawler = WebCrawler('config.json')

# Start crawling
crawler.start_crawling()

# Check status
status = crawler.get_status()
print(f"Pages crawled: {status['pages_crawled']}")

# Stop crawling
crawler.stop_crawling()
```

## Architecture

### Core Components

1. **URLFrontier** (`url_frontier.py`)
   - Thread-safe FIFO queue
   - Configurable maximum size
   - Queue management operations

2. **HTMLDownloader** (`html_downloader.py`)
   - HTTP requests with retry logic
   - Configurable timeout and delays
   - Error handling and logging

3. **RobotsParser** (`robots_parser.py`)
   - Fetches and parses robots.txt files
   - Enforces crawl delays and disallowed paths
   - Domain-specific rule management

4. **LinkExtractor** (`link_extractor.py`)
   - Extracts `<a href>` links from HTML
   - URL normalization and validation
   - Filters unwanted file types

5. **URLSeen** (`url_seen.py`)
   - HashSet for exact deduplication
   - Bloom filter for quick negative checks
   - Memory-efficient URL tracking

6. **ContentStorage** (`content_storage.py`)
   - SQLite database for metadata
   - HTML file storage
   - Statistics and reporting

7. **CrawlerLogger** (`logger.py`)
   - Centralized logging
   - Real-time statistics
   - Performance metrics

### Database Schema

The SQLite database includes two main tables:

#### `pages` table
- `id`: Primary key
- `url`: Page URL (unique)
- `html_path`: Path to stored HTML file
- `status_code`: HTTP response code
- `content_length`: Size of HTML content
- `title`: Page title
- `domain`: Domain name
- `metadata`: JSON metadata (headers, etc.)
- `timestamp`: Crawl timestamp
- `crawl_duration`: Time taken to crawl

#### `crawl_stats` table
- `id`: Primary key
- `total_pages`: Total pages crawled
- `successful_pages`: Successfully crawled pages
- `failed_pages`: Failed pages
- `total_links_found`: Total links discovered
- `queue_size`: Current queue size
- `timestamp`: Statistics timestamp

## Output

### Files
- **HTML Files**: Stored in `crawled_data/` directory
- **Database**: SQLite database at `crawler.db`
- **Logs**: Console output and `crawler.log` file

### Statistics
The crawler prints statistics every 10 seconds (configurable):
- Runtime and crawl rates
- Pages crawled, successful, and failed
- Links found and queue size
- Error counts and success rates

## Extensibility

### Adding New Components

1. **Custom Downloaders**: Extend `HTMLDownloader` class
2. **Custom Storage**: Implement new storage backends
3. **Custom Filters**: Add URL or content filters
4. **Custom Extractors**: Extract different types of data

### Example: Custom URL Filter

```python
class CustomURLFilter:
    def should_crawl(self, url):
        # Add custom logic
        return not url.endswith('.pdf')

# Integrate into crawler
crawler.url_filter = CustomURLFilter()
```

## Error Handling

- **Network Errors**: Automatic retries with exponential backoff
- **Invalid URLs**: Validation and filtering
- **Storage Errors**: Graceful degradation
- **Robots.txt Errors**: Default to allowing access

## Performance Considerations

- **Memory Usage**: Bloom filter reduces memory footprint
- **Database**: Indexed queries for fast lookups
- **Concurrency**: Thread-safe operations
- **Rate Limiting**: Respects crawl delays and robots.txt

## Troubleshooting

### Common Issues

1. **Queue Full**: Increase `max_queue_size` in config
2. **Memory Issues**: Reduce `capacity` in URLSeen
3. **Slow Crawling**: Check robots.txt delays
4. **Database Locked**: Ensure only one crawler instance

### Debug Mode

Enable debug logging by modifying the logger initialization:

```python
self.logger = CrawlerLogger(log_level="DEBUG")
```

## License

This project is open source and available under the MIT License.
