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
    "db_path": "crawler.db",
    "kafka": { "bootstrap_servers": ["localhost:9092"] }
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
- `kafka.bootstrap_servers`: Kafka brokers for priority mode

## Usage

### Dockerized Usage (Recommended)

```bash
# Start all services with one command
./start.sh

# View logs
./logs.sh

# Stop all services
./stop.sh
```

### Manual Docker Commands

```bash
# Start all services
docker-compose up --build

# View logs from all services
docker-compose logs -f

# View logs from specific service
docker-compose logs -f master
docker-compose logs -f worker-1

# Stop all services
docker-compose down
```

For detailed Docker setup instructions, see [DOCKER_README.md](DOCKER_README.md).

## Getting Started Guide

New to the project? Read the step-by-step guide: [GETTING_STARTED.md](GETTING_STARTED.md)

## Architecture

### Project Structure

```
crawler/
├── main.py                     # Main entry point
├── config.json                 # Configuration file
├── requirements.txt            # Dependencies
├── src/
│   └── crawler/
│       ├── core/              # Core crawler components
│       │   ├── master.py      # Master dispatcher to Kafka topics
│       │   ├── worker.py      # Worker consumes from priority topic
│       │   └── html_downloader.py # HTML downloading
│       ├── storage/           # Storage components
│       │   └── content_storage.py # MongoDB storage
│       ├── parsing/           # Parsing components
│       │   ├── robots_parser.py # Robots.txt compliance
│       │   └── link_extractor.py # Link extraction
│       ├── utils/             # Utility components
│       │   └── logger.py      # Logging and metrics
│       └── prioritizer.py     # URL priority assignment (random 1-5)
├── tests/                     # Test files
└── crawled_data/             # Output directory
```

### Core Components

1. **Kafka Worker** (`src/crawler/core/worker.py`)
   - Consumes URLs from `urls_priority_*` topics
   - Downloads, stores HTML, extracts links
   - Re-enqueues discovered links by priority

2. **HTMLDownloader** (`src/crawler/core/html_downloader.py`)
   - HTTP requests with retry logic
   - Configurable timeout and delays
   - Error handling and logging

3. **LinkExtractor** (`src/crawler/parsing/link_extractor.py`)
   - Extracts `<a href>` links from HTML
   - URL normalization and validation

4. **LinkExtractor** (`src/crawler/parsing/link_extractor.py`)
   - Extracts `<a href>` links from HTML
   - URL normalization and validation
   - Filters unwanted file types

5. **ContentStorage** (`src/crawler/storage/content_storage.py`)
   - MongoDB database for metadata
   - HTML file storage

6. **ContentStorage** (`src/crawler/storage/content_storage.py`)
   - MongoDB database for metadata
   - HTML file storage
   - Statistics and reporting

7. **CrawlerLogger** (`src/crawler/utils/logger.py`)
   - Centralized logging
   - Real-time statistics
   - Performance metrics

### Priority Queue Mode (Kafka)

- Master publishes URLs to 5 Kafka topics: `urls_priority_1` ... `urls_priority_5` (5 = highest).
- Prioritizer returns an integer priority 1-5 for each URL.
- Five workers each consume from a dedicated topic and process URLs independently.

Run with Kafka (ensure Kafka at `localhost:9092` or set `kafka.bootstrap_servers` in `config.json`):

```
python -m src.crawler.core.master
python -m src.crawler.core.worker urls_priority_1
python -m src.crawler.core.worker urls_priority_2
python -m src.crawler.core.worker urls_priority_3
python -m src.crawler.core.worker urls_priority_4
python -m src.crawler.core.worker urls_priority_5
```

## Database Schema

The MongoDB database includes two main collections:

#### `pages` collection
- `_id`: MongoDB ObjectId (auto-generated)
- `url`: Page URL (unique index)
- `html_path`: Path to stored HTML file
- `status_code`: HTTP response code
- `content_length`: Size of HTML content
- `title`: Page title
- `domain`: Domain name (indexed)
- `metadata`: Object containing headers and other metadata
- `content_hash`: SHA-256 hash of content (indexed)
- `timestamp`: Crawl timestamp (indexed)
- `crawl_duration`: Time taken to crawl

#### `crawl_stats` collection
- `_id`: MongoDB ObjectId (auto-generated)
- `total_pages`: Total pages crawled
- `successful_pages`: Successfully crawled pages
- `failed_pages`: Failed pages
- `total_links_found`: Total links discovered
- `queue_size`: Current queue size
- `timestamp`: Statistics timestamp (indexed)

## Output

### Files
- **HTML Files**: Stored in `crawled_data/` directory
- **Database**: MongoDB database (configurable connection)
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
