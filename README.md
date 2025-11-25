# Distributed Real Estate Web Crawler

A distributed, geospatially-aware web crawler system designed for property listings with priority-based URL processing, built on Kafka, PostgreSQL and Docker.

## System Design

![System Design Diagram](./diagrams/diagram.png)

## Features

- **Distributed Architecture** - Master-worker pattern with Kafka-based message queuing
- **Priority-Based Processing** - 5-level priority system with geospatial awareness
- **Geospatial Prioritization** - Automatically assigns priorities based on geographic regions and listing types
- **Property-Specific Crawling** - Specialized parsing for property listings (Craigslist)
- **PostgreSQL Storage** - PostGIS-enabled database for geospatial queries
- **RESTful API** - FastAPI service for querying crawled property data
- **CLI Control** - Command-line interface for controlling the crawler
- **Robots.txt Compliance** - Respects robots.txt rules and crawl delays
- **Bloom Filter Deduplication** - Efficient URL deduplication using Bloom filters
- **Dockerized Deployment** - Complete containerized setup with Docker Compose

## Architecture

### Workflow

![Workflow](./diagrams/workflow.png)

### System Overview

The crawler follows a **distributed master-worker architecture** with the following components:

1. **Master Service** - CLI-controlled URL dispatcher that sends seed URLs to Kafka priority queues
2. **Worker Services** (11 workers) - Consume URLs from Kafka topics and perform web crawling
3. **API Service** - FastAPI for querying crawled property data
4. **Kafka** - Message broker with 5 priority topics for URL distribution
5. **Zookeeper** - Coordination service for Kafka
6. **PostgreSQL** - Database with PostGIS extension for storing property data
7. **Geospatial Prioritizer** - Assigns priorities based on geographic regions and URL patterns

### Project Structure

```
Crawler/
├── api/                          
│   ├── Dockerfile
│   └── main.py                   
├── master/                      
│   ├── Dockerfile
│   ├── main.py                   # CLI interface (start, health_check, stop, add_url)
│   ├── requirements.txt
│   └── src/
│       └── crawler/
│           ├── core/
│           │   └── master.py     
│           ├── queue_manager.py 
│           └── robots_checker.py
├── worker/                       # Worker Service
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       └── crawler/
│           ├── core/
│           │   ├── worker.py           
│           │   └── html_downloader.py  
│           ├── parsing/
│           │   ├── craigslist_parser.py 
│           │   └── link_extractor.py  
│           ├── storage/
│           │   ├── content_storage.py  
│           │   └── database_service.py 
│           └── utils/
│               ├── logger.py          
│               └── property_matcher.py  
├── geospatial/                   
│   ├── generate_coords.py      
│   ├── prioritizer.py        
│   └── state_coords.json       
├── config.json                 
├── docker-compose.yml        
├── start.sh                    
├── stop.sh                             
└── requirements.txt                      
```

## Services Explained

### 1. Master Service (`master/`)

**Purpose**: CLI-controlled URL dispatcher that manages crawling operations.

**Components**:
- **`main.py`**: Command-line interface providing:
  - `start` - Dispatch seed URLs from config to Kafka topics
  - `health_check` - Verify all services (Kafka, workers, PostgreSQL)
  - `stop` - Get instructions to stop crawling
  - `add_url` - Manually add URLs to specific priority queues
  - `queue_info` - Display queue configuration information
- **`master.py`**: Core dispatcher that reads seed URLs from config, assigns priorities using the geospatial prioritizer, and sends them to appropriate Kafka topics
- **`queue_manager.py`**: Manages manual URL additions with validation and robots.txt checking
- **`robots_checker.py`**: Validates URLs against robots.txt before enqueueing

**Key Features**:
- Controlled mode: Only dispatches when explicitly commanded
- Priority assignment via geospatial prioritizer
- Robots.txt validation before dispatch
- Health monitoring of all services

### 2. Worker Service (`worker/`)

**Purpose**: Consume URLs from Kafka topics and perform web crawling operations.

**Components**:
- **`worker.py`**: Main worker process that:
  - Subscribes to a specific Kafka priority topic
  - Processes URLs in FIFO order
  - Downloads HTML content
  - Extracts property data using Craigslist parser
  - Saves property pages to PostgreSQL
  - Extracts links and re-enqueues them with appropriate priorities
  - Uses Bloom filter for deduplication
- **`html_downloader.py`**: Handles HTTP requests with:
  - Configurable retries with exponential backoff
  - Timeout management
  - Request delays and minimum intervals
  - Headless browser support (Selenium/Playwright)
- **`craigslist_parser.py`**: Extracts property data from Craigslist HTML:
  - Title, price, address
  - Geographic coordinates (latitude/longitude)
  - Property type, bedrooms, bathrooms, square footage
  - City, posted date
- **`link_extractor.py`**: Extracts and normalizes links from HTML
- **`database_service.py`**: PostgreSQL storage interface:
  - Saves property pages with full metadata
  - Stores HTML content
  - PostGIS geospatial indexing
  - Deduplication checks
- **`property_matcher.py`**: Determines if URLs are property listings or listing pages
- **`logger.py`**: Centralized logging with metrics

**Key Features**:
- 11 workers distributed across 5 priority topics (1-2 partitions per topic)
- Only saves property pages to database (skips listing pages)
- Strict FIFO ordering within each partition
- Automatic link discovery and priority assignment
- Bloom filter for fast deduplication

### 3. API Service (`api/`)

**Purpose**: RESTful API for querying crawled property data.

**Endpoints**:
- `GET /` - API welcome message
- `GET /properties` - List all properties with pagination
- `GET /properties/{id}` - Get specific property by ID
- `GET /properties/nearby` - Find properties within radius of coordinates
- `GET /properties/similar/{property_id}` - Find similar properties (price, location, type)
- `GET /properties/similar/nearby` - Find properties matching criteria within radius
- `GET /properties/bbox` - Get properties within bounding box
- `GET /properties/city/{city}` - Get properties by city name
- `GET /properties/heatmap` - Get property density heatmap data
- `GET /location/city/{city}` - Get coordinates for a city

**Key Features**:
- PostGIS-powered geospatial queries
- CORS enabled for web frontends
- Efficient pagination
- Filtering by price, property type, location
- Heatmap generation for visualization

### 4. Geospatial Prioritizer (`geospatial/prioritizer.py`)

**Purpose**: Assigns priorities to URLs based on geographic regions and URL patterns.

**Priority Assignment Logic**:
- **Priority 1-2**: Listing pages (`/search/apa`), alternated for load balancing
- **Priority 3-5**: Property pages, assigned by geographic region:
  - Priority 3: Eastern US states (lowest longitude)
  - Priority 4: Central US states (medium longitude)
  - Priority 5: Western US states (highest longitude)

**Features**:
- Maps Craigslist cities to US states
- Caches state coordinates to avoid repeated geocoding
- Divides US states into 3 geographic regions by longitude
- Filters non-target domains (returns -1)
- Uses round-robin for listing page priorities

### 5. Kafka Message Broker

**Topics**:
- `urls_priority_1`: 1 partition (lowest priority - listing pages)
- `urls_priority_2`: 1 partition (lowest priority - listing pages)
- `urls_priority_3`: 3 partitions (eastern US - property pages)
- `urls_priority_4`: 3 partitions (central US - property pages)
- `urls_priority_5`: 3 partitions (western US - property pages)

**Configuration**:
- Partitions enable parallel processing within each priority level
- Workers subscribe to specific topics
- FIFO ordering guaranteed within each partition
- Auto-commit disabled for manual offset management

### 6. PostgreSQL Database

**Schema** (`public.pages` table):
- `id` - Primary key (SERIAL)
- `url` - Unique URL (TEXT)
- `title` - Property title
- `price` - Property price (NUMERIC)
- `property_type` - Type of property
- `city` - City name
- `latitude` / `longitude` - Geographic coordinates
- `geohash` - PostGIS GEOGRAPHY(POINT) for spatial queries
- `beds` / `baths` / `sqft` - Property details
- `image_path` - Path to property image
- `status_code` - HTTP response code
- `headers` - JSONB metadata
- `html_content` - Full HTML (TEXT)
- `crawl_duration` - Time taken to crawl
- `created_at` - Timestamp

**Features**:
- PostGIS extension enabled for geospatial queries
- Unique constraint on URL for deduplication
- Indexed columns for fast queries
- JSONB for flexible metadata storage

## Configuration

Edit `config.json` to customize crawler behavior:

```json
{
    "seed_urls": [],
    "max_pages": 25,
    "max_workers": 3,
    "default_priority": 3,
    "delay_between_requests": 3.0,
    "min_request_interval": 8.0,
    "timeout": 60,
    "max_retries": 3,
    "headless": true,
    "use_proxy": false,
    "respect_robots": true,
    "max_queue_size": 100,
    "stats_interval": 10,
    "output_dir": "/mnt/storage",
    "database": {
        "connection_string": "postgresql://user:password@host:5432/postgres",
        "database_name": "postgres"
    },
    "kafka": {
        "bootstrap_servers": ["kafka:9092"]
    }
}
```

### Configuration Options

- `seed_urls`: List of starting URLs to crawl (empty array uses manual URL addition)
- `max_pages`: Maximum number of pages to crawl
- `max_workers`: Maximum concurrent workers (informational)
- `default_priority`: Default priority (1-5) for manually added URLs
- `delay_between_requests`: Delay between requests in seconds
- `min_request_interval`: Minimum time between requests to same domain
- `timeout`: Request timeout in seconds
- `max_retries`: Maximum retry attempts for failed requests
- `headless`: Use headless browser mode
- `use_proxy`: Enable proxy support
- `respect_robots`: Whether to respect robots.txt rules
- `max_queue_size`: Maximum size of URL queue (informational for Kafka)
- `stats_interval`: Interval for printing statistics (seconds)
- `output_dir`: Directory to store crawled HTML files
- `database.connection_string`: PostgreSQL connection string
- `database.database_name`: Database name
- `kafka.bootstrap_servers`: Kafka broker addresses

## Installation & Usage

### Prerequisites

- Docker
- Docker Compose

### Quick Start

1. **Start all services**:
```bash
./start.sh
```

This starts:
- Zookeeper
- Kafka (with topics auto-created)
- Master service (CLI container)
- 11 Worker services
- API service (port 8000)
- PostgreSQL (external service required)

2. **Control the crawler via CLI**:
```bash
# Check system health
docker exec -it crawler-master python3 main.py health_check

# Start crawling (dispatches seed URLs from config.json)
docker exec -it crawler-master python3 main.py start

# Manually add URLs
docker exec -it crawler-master python3 main.py add_url --urls "https://example.com" --priority 5

# Get queue information
docker exec -it crawler-master python3 main.py queue_info

# Stop instructions
docker exec -it crawler-master python3 main.py stop
```

3. **Access the API**:
```bash
# List properties
curl http://localhost:8000/properties

# Find nearby properties
curl "http://localhost:8000/properties/nearby?lat=37.7749&lon=-122.4194&radius=5000"

# Get property by ID
curl http://localhost:8000/properties/1
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `python3 main.py start` | Dispatch seed URLs from config.json to Kafka queues |
| `python3 main.py health_check` | Check status of Kafka, workers, and PostgreSQL |
| `python3 main.py stop` | Get instructions to stop crawling |
| `python3 main.py add_url --urls <url1> [url2] ...` | Add URLs to queue (optional: `--priority N`, `--queue QUEUE`) |
| `python3 main.py queue_info` | Display queue configuration |

### Docker Management

```bash
# Start all services
docker-compose up --build

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f master
docker-compose logs -f worker-1
docker-compose logs -f api
docker-compose logs -f kafka

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### Health Check Details

The health check command verifies:
- **Kafka Connection**: Verifies Kafka brokers are accessible
- **Kafka Topics**: Checks if priority topics exist and shows partition counts
- **Worker Containers**: Shows status of all worker containers
- **Master Container**: Shows status of the master dispatcher
- **PostgreSQL Connection**: Verifies database connectivity

## Priority System

The crawler uses a 5-level priority system:

1. **Priority 1-2** (Lowest): Listing pages (`/search/apa`) - used for discovering property URLs
2. **Priority 3-5** (Higher): Property pages, assigned by geographic region:
   - **Priority 3**: Eastern US states
   - **Priority 4**: Central US states  
   - **Priority 5**: Western US states

Priority assignment is automatic based on:
- URL pattern (listing vs property page)
- Geographic region (derived from city → state → longitude)
- Round-robin for listing pages (alternates between 1 and 2)

## Workflow

1. **Seed URLs** are configured in `config.json` or added via CLI
2. **Master** dispatches URLs to Kafka topics based on priority
3. **Workers** consume URLs from their assigned topics
4. **HTML Downloader** fetches page content with retries and delays
5. **Property Matcher** determines if URL is a property page
6. **Craigslist Parser** extracts property data from property pages
7. **Database Service** saves property pages to PostgreSQL
8. **Link Extractor** finds new URLs from HTML
9. **Prioritizer** assigns priorities to discovered URLs
10. **Workers** re-enqueue discovered URLs to appropriate topics
11. **API Service** provides RESTful access to crawled data

## Database Schema

### `public.pages` Table

Stores all crawled property pages with full metadata:

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key |
| `url` | TEXT | Unique URL (indexed) |
| `title` | TEXT | Property title |
| `price` | NUMERIC | Property price |
| `property_type` | TEXT | Type (apartment, house, etc.) |
| `city` | TEXT | City name |
| `latitude` | DOUBLE PRECISION | Latitude coordinate |
| `longitude` | DOUBLE PRECISION | Longitude coordinate |
| `geohash` | GEOGRAPHY(POINT) | PostGIS point for spatial queries |
| `beds` | INTEGER | Number of bedrooms |
| `baths` | INTEGER | Number of bathrooms |
| `sqft` | INTEGER | Square footage |
| `image_path` | TEXT | Path to property image |
| `status_code` | INTEGER | HTTP response code |
| `headers` | JSONB | Response headers |
| `html_content` | TEXT | Full HTML content |
| `crawl_duration` | DOUBLE PRECISION | Time taken to crawl |
| `created_at` | TIMESTAMP | Crawl timestamp (indexed) |

**Indexes**:
- Primary key on `id`
- Unique constraint on `url`
- Spatial index on `geohash` (PostGIS)

## API Documentation

### Base URL
```
http://localhost:8000
```

### Key Endpoints

#### List Properties
```http
GET /properties?limit=100&offset=0
```

#### Find Nearby Properties
```http
GET /properties/nearby?lat=37.7749&lon=-122.4194&radius=5000&limit=100
```

#### Find Similar Properties
```http
GET /properties/similar/123?price_diff=50000&radius=2000&limit=10
```

#### Properties by Bounding Box
```http
GET /properties/bbox?sw_lat=37.0&sw_lng=-123.0&ne_lat=38.0&ne_lng=-122.0&limit=1000
```

#### Property Heatmap
```http
GET /properties/heatmap?sw_lat=37.0&sw_lng=-123.0&ne_lat=38.0&ne_lng=-122.0&grid_size=0.01
```

See the API service code (`api/main.py`) for complete endpoint documentation.

## Performance Considerations

- **Bloom Filter**: Reduces memory footprint for URL deduplication
- **Partitioning**: Kafka topics are partitioned for parallel processing
- **PostGIS Indexing**: Spatial queries use indexed geohash column
- **FIFO Ordering**: Strict ordering within partitions ensures consistent processing
- **Rate Limiting**: Configurable delays prevent overwhelming target servers
- **Connection Pooling**: Database connections are pooled and reused

## Error Handling

- **Network Errors**: Automatic retries with exponential backoff
- **Invalid URLs**: Validation and filtering before processing
- **Database Errors**: Graceful error handling with rollback
- **Robots.txt Errors**: Defaults to allowing access if check fails
- **Kafka Errors**: Retry logic with backoff for connection issues

## Troubleshooting

### Common Issues

1. **Kafka Connection Failed**: Ensure Kafka service is running (`docker-compose ps`)
2. **PostgreSQL Connection Failed**: Verify connection string in `config.json`
3. **Workers Not Processing**: Check if URLs are in Kafka topics (`docker-compose logs worker-1`)
4. **API Not Responding**: Verify API container is running (`docker-compose logs api`)
5. **No Properties in Database**: Ensure seed URLs are Craigslist property listings

### Debug Mode

Enable verbose logging by checking worker logs:
```bash
docker-compose logs -f worker-1
```

### Viewing Kafka Topics

```bash
# Enter Kafka container
docker exec -it kafka bash

# List topics
kafka-topics --list --bootstrap-server localhost:9092

# View messages in topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic urls_priority_5 --from-beginning
```

## Extensibility

### Adding New Parsers

Create a new parser in `worker/src/crawler/parsing/` and integrate it into `worker.py`.

### Custom Priority Logic

Modify `geospatial/prioritizer.py` to implement custom priority assignment.

### Additional API Endpoints

Add new endpoints in `api/main.py` following the FastAPI pattern.

## License

This project is open source and available under the MIT License.
