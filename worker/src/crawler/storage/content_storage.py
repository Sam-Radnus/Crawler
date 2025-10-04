"""
Content Storage - MongoDB database for storing crawled content
"""
import os
import json
import logging
import re
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError


class ContentStorage:
    """MongoDB-based storage for crawled content and metadata"""
    
    def __init__(self, output_dir: str = "crawled_data", 
                 connection_string: str = "mongodb://localhost:27017/", 
                 database_name: str = "web_crawler"):
        self.output_dir = output_dir
        self.connection_string = connection_string
        self.database_name = database_name
        self.logger = logging.getLogger(__name__)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize MongoDB connection
        self._init_database()
    
    def _init_database(self):
        """Initialize MongoDB database with required collections and indexes"""
        try:
            # Connect to MongoDB
            self.client = MongoClient(self.connection_string, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            
            # Get database
            self.db = self.client[self.database_name]
            
            # Get collections
            self.pages_collection = self.db.pages
            self.crawl_stats_collection = self.db.crawl_stats
            self.visited_pages_collection = self.db.visited_pages
            
            # Create indexes for better performance
            self.pages_collection.create_index("url", unique=True)
            
            self.crawl_stats_collection.create_index("timestamp")
            self.visited_pages_collection.create_index("url", unique=True)
            self.visited_pages_collection.create_index("domain")
            self.visited_pages_collection.create_index("timestamp")
            
            self.logger.info(f"MongoDB database initialized: {self.database_name}")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error initializing MongoDB database: {e}")
            raise

    def save_page(self, url: str, html_content: str, status_code: int, 
                  headers: Dict[str, str], crawl_duration: float) -> bool:
        """
        Save crawled page to MongoDB and filesystem
        
        Args:
            url: Page URL
            html_content: HTML content
            status_code: HTTP status code
            headers: HTTP response headers
            crawl_duration: Time taken to crawl the page
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            # Generate filename for HTML content
            filename = self._generate_filename(url)
            html_path = os.path.join(self.output_dir, filename)
            
            # Save HTML content to file
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Extract title from HTML
            title = self._extract_title(html_content)
            
            # Generate content hash
            content_hash = self._generate_content_hash(html_content)
            
            # Prepare metadata
            metadata = {
                'headers': headers,
                'content_type': headers.get('content-type', ''),
                'server': headers.get('server', ''),
                'last_modified': headers.get('last-modified', ''),
                'content_encoding': headers.get('content-encoding', '')
            }
            
            # Prepare document for MongoDB
            document = {
                'url': url,
                'html_path': html_path,
                'status_code': status_code,
                'content_length': len(html_content),
                'title': title,
                'domain': domain,
                'metadata': metadata,
                'content_hash': content_hash,
                'timestamp': datetime.utcnow(),
                'crawl_duration': crawl_duration
            }
            
            # Save to MongoDB (upsert to handle duplicates)
            self.pages_collection.replace_one(
                {'url': url},  # Filter
                document,      # Replacement document
                upsert=True    # Insert if not exists
            )
            
            self.logger.info(f"Saved page: {url} -> {html_path}")
            return True
            
        except DuplicateKeyError as e:
            self.logger.warning(f"Duplicate key error for {url}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error saving page {url}: {e}")
            return False
    
    def _generate_filename(self, url: str) -> str:
        """Generate safe filename from URL"""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        
        if not path:
            path = 'index'
        
        # Replace invalid filename characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', path)
        filename = filename[:200]  # Limit length
        
        # Add timestamp to avoid conflicts
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{parsed.netloc}_{filename}_{timestamp}.html"
    
    def _extract_title(self, html_content: str) -> Optional[str]:
        """Extract title from HTML content"""
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            title_tag = soup.find('title')
            return title_tag.get_text().strip() if title_tag else None
        except Exception:
            return None
    
    def _generate_content_hash(self, html_content: str) -> str:
        """Generate SHA-256 hash of HTML content"""
        return hashlib.sha256(html_content.encode('utf-8')).hexdigest()
    
    def has_content_hash(self, content_hash: str) -> bool:
        """Check if content hash already exists in database"""
        try:
            count = self.pages_collection.count_documents({'content_hash': content_hash}, limit=1)
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking content hash: {e}")
            return False
    
    
    def get_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Retrieve page data from database"""
        try:
            document = self.pages_collection.find_one({'url': url})
            if document:
                # Convert ObjectId to string and handle datetime
                document['_id'] = str(document['_id'])
                if 'timestamp' in document and document['timestamp']:
                    document['timestamp'] = document['timestamp'].isoformat()
                return document
            return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving page {url}: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get crawling statistics"""
        try:
            # Get page counts
            total_pages = self.pages_collection.count_documents({})
            successful_pages = self.pages_collection.count_documents({'status_code': 200})
            failed_pages = self.pages_collection.count_documents({'status_code': {'$ne': 200}})
            
            # Get domain distribution using aggregation
            pipeline = [
                {'$group': {'_id': '$domain', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            domain_stats = {}
            for doc in self.pages_collection.aggregate(pipeline):
                domain_stats[doc['_id']] = doc['count']
            
            return {
                'total_pages': total_pages,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'success_rate': (successful_pages / total_pages * 100) if total_pages > 0 else 0,
                'domain_distribution': domain_stats
            }
                
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return {}
    
    def save_crawl_stats(self, total_pages: int, successful_pages: int, 
                        failed_pages: int, total_links: int, queue_size: int):
        """Save current crawl statistics"""
        try:
            stats_document = {
                'total_pages': total_pages,
                'successful_pages': successful_pages,
                'failed_pages': failed_pages,
                'total_links_found': total_links,
                'queue_size': queue_size,
                'timestamp': datetime.utcnow()
            }
            
            self.crawl_stats_collection.insert_one(stats_document)
                
        except Exception as e:
            self.logger.error(f"Error saving crawl stats: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        try:
            if hasattr(self, 'client'):
                self.client.close()
                self.logger.info("MongoDB connection closed")
        except Exception as e:
            self.logger.error(f"Error closing MongoDB connection: {e}")
    
    def __del__(self):
        """Destructor to ensure connection is closed"""
        self.close_connection()