# MongoDB Setup Guide

The web crawler now uses MongoDB as the default database. This guide will help you set up MongoDB and run the crawler.

## Quick Start

### 1. Install MongoDB

#### Option A: Using Docker (Recommended)
```bash
# Start MongoDB in a Docker container
docker run -d -p 27017:27017 --name mongodb mongo:latest

# Check if MongoDB is running
docker ps
```

#### Option B: Install MongoDB Locally
- **macOS**: `brew install mongodb-community`
- **Ubuntu**: `sudo apt-get install mongodb`
- **Windows**: Download from https://www.mongodb.com/try/download/community

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Test MongoDB Connection
```bash
python3 test_mongodb_only.py
```

### 4. Run the Crawler
```bash
# Soft mode (URL-only deduplication)
python3 web_crawler.py --mode soft

# Hard mode (URL + content hash deduplication)
python3 web_crawler.py --mode hard
```

## Configuration

The crawler uses MongoDB with these default settings:

```json
{
    "database": {
        "connection_string": "mongodb://localhost:27017/",
        "database_name": "web_crawler"
    }
}
```

### Custom Configuration

You can customize the MongoDB connection in `config.json`:

```json
{
    "seed_urls": ["https://example.com"],
    "max_pages": 100,
    "max_workers": 3,
    "delay_between_requests": 1.0,
    "timeout": 10,
    "max_retries": 3,
    "user_agent": "WebCrawler/1.0",
    "respect_robots": true,
    "max_queue_size": 100,
    "stats_interval": 10,
    "output_dir": "crawled_data",
    "database": {
        "connection_string": "mongodb://username:password@localhost:27017/",
        "database_name": "my_web_crawler"
    }
}
```

## MongoDB Collections

The crawler creates two collections:

### 1. `pages` Collection
Stores crawled page data:
```javascript
{
  _id: ObjectId,
  url: String (unique),
  html_path: String,
  status_code: Number,
  content_length: Number,
  title: String,
  domain: String,
  metadata: Object,
  content_hash: String,
  timestamp: Date,
  crawl_duration: Number
}
```

### 2. `crawl_stats` Collection
Stores crawling statistics:
```javascript
{
  _id: ObjectId,
  total_pages: Number,
  successful_pages: Number,
  failed_pages: Number,
  total_links_found: Number,
  queue_size: Number,
  timestamp: Date
}
```

## Indexes

The following indexes are automatically created for optimal performance:

- `url` (unique)
- `domain`
- `timestamp`
- `content_hash`
- `url + content_hash` (compound)

## Troubleshooting

### Connection Issues

1. **MongoDB not running**:
   ```bash
   # Check if MongoDB is running
   docker ps | grep mongo
   
   # Start MongoDB if not running
   docker start mongodb
   ```

2. **Connection refused**:
   - Check if MongoDB is running on port 27017
   - Verify the connection string in config.json

3. **Authentication failed**:
   - Check username/password in connection string
   - Ensure MongoDB authentication is properly configured

### Performance Issues

1. **Slow queries**: Check if indexes are created properly
2. **High memory usage**: Adjust MongoDB cache size
3. **Slow writes**: Consider using write concerns

## Monitoring

### Check MongoDB Status
```bash
# Connect to MongoDB shell
docker exec -it mongodb mongosh

# Show databases
show dbs

# Use web_crawler database
use web_crawler

# Show collections
show collections

# Count pages
db.pages.countDocuments()

# Show recent pages
db.pages.find().sort({timestamp: -1}).limit(5)
```

### View Crawler Logs
```bash
tail -f crawler.log
```

## Backup and Recovery

### Backup Database
```bash
# Backup to local directory
docker exec mongodb mongodump --out /backup

# Copy backup from container
docker cp mongodb:/backup ./mongodb_backup
```

### Restore Database
```bash
# Copy backup to container
docker cp ./mongodb_backup mongodb:/restore

# Restore database
docker exec mongodb mongorestore /restore
```

## Production Considerations

For production use, consider:

1. **Replica Sets**: For high availability
2. **Sharding**: For horizontal scaling
3. **Authentication**: Enable MongoDB authentication
4. **SSL/TLS**: Use encrypted connections
5. **Monitoring**: Set up MongoDB monitoring tools

## Support

If you encounter issues:

1. Check MongoDB logs: `docker logs mongodb`
2. Check crawler logs: `crawler.log`
3. Verify configuration: `config.json`
4. Test connection: `python3 test_mongodb_only.py`
