# Dockerized Web Crawler

This document explains how to run the web crawler using Docker containers.

## Architecture

The application is split into the following containers:

- **Master Container**: Dispatches URLs to Kafka priority topics
- **5 Worker Containers**: Each consumes from a specific priority topic (urls_priority_1 to urls_priority_5)
- **Kafka + Zookeeper**: Message broker for URL distribution
- **MongoDB**: Database for storing crawled content

## Quick Start

### Prerequisites

- Docker
- Docker Compose

### Single Command Deployment

```bash
./start.sh
```

This will:
1. Build all Docker images
2. Start all services (MongoDB, Kafka, Master, 5 Workers)
3. Display real-time logs

### Manual Commands

```bash
# Start all services
docker-compose up --build

# Start in background
docker-compose up --build -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## Service Management

### View Logs

```bash
# All services
./logs.sh

# Specific service
./logs.sh master
./logs.sh worker-1
./logs.sh kafka
./logs.sh mongodb
```

### Stop Services

```bash
./stop.sh
```

## Configuration

### Master Configuration
Located in `master/config.json`:
- Seed URLs
- Kafka bootstrap servers (kafka:29092)
- MongoDB connection (mongodb://mongodb:27017/)

### Worker Configuration
Located in `worker/config.json`:
- Same as master but for worker processes
- Each worker consumes from a specific priority topic

## Container Details

### Master Container
- **Image**: Built from `master/Dockerfile`
- **Purpose**: Dispatches seed URLs to Kafka topics based on priority
- **Dependencies**: Kafka, MongoDB

### Worker Containers
- **Image**: Built from `worker/Dockerfile`
- **Purpose**: Consumes URLs from specific Kafka topics and crawls them
- **Topics**: urls_priority_1, urls_priority_2, urls_priority_3, urls_priority_4, urls_priority_5
- **Dependencies**: Kafka, MongoDB

### Kafka + Zookeeper
- **Image**: confluentinc/cp-kafka:7.4.0
- **Port**: 9092 (external), 29092 (internal)
- **Auto-creates topics**: urls_priority_1 to urls_priority_5

### MongoDB
- **Image**: mongo:7.0
- **Port**: 27017
- **Database**: web_crawler
- **Data persistence**: Yes (docker volume)

## Monitoring

### Real-time Statistics
All containers output statistics to console every 10 seconds:
- Pages crawled
- Success/failure rates
- Links found
- Queue sizes
- Error counts

### Database Access
```bash
# Connect to MongoDB
docker exec -it mongodb mongosh

# Use web_crawler database
use web_crawler

# View collections
show collections

# View pages
db.pages.find().limit(5)
```

### Kafka Topics
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# View topic details
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic urls_priority_1
```

## Troubleshooting

### Common Issues

1. **Port conflicts**: Ensure ports 9092, 27017, and 2181 are available
2. **Memory issues**: Increase Docker memory allocation
3. **Network issues**: Restart Docker daemon

### Debug Commands

```bash
# Check container status
docker-compose ps

# Check container logs
docker-compose logs <service-name>

# Execute commands in container
docker exec -it <container-name> /bin/bash

# Check Kafka connectivity
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Reset Everything

```bash
# Stop and remove all containers, networks, and volumes
docker-compose down -v

# Remove all images
docker-compose down --rmi all

# Clean up system
docker system prune -a
```

## Performance Tuning

### Scaling Workers
To add more workers, modify `docker-compose.yml`:

```yaml
worker-6:
  build:
    context: ./worker
    dockerfile: Dockerfile
  container_name: crawler-worker-6
  depends_on:
    - kafka
    - mongodb
  environment:
    - PYTHONUNBUFFERED=1
  command: ["python", "-m", "src.crawler.core.worker", "urls_priority_1"]
  networks:
    - crawler-network
  restart: unless-stopped
```

### Resource Limits
Add resource limits to containers in `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
```

## Data Persistence

- **MongoDB data**: Persisted in Docker volume `mongodb_data`
- **Crawled HTML files**: Stored in `crawled_data/` directory
- **Logs**: Output to console (no file storage)

## Security Notes

- Containers run in isolated network
- No external ports exposed except for monitoring
- MongoDB accessible only from within Docker network
- Kafka accessible only from within Docker network
