"""
Content Storage - SQLite database for storing crawled content
"""
import sqlite3
import os
import json
import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse


class ContentStorage:
    """SQLite-based storage for crawled content and metadata"""
    
    def __init__(self, db_path: str = "crawler.db", output_dir: str = "crawled_data"):
        self.db_path = db_path
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database with required schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create main table for crawled pages
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL,
                        html_path TEXT,
                        status_code INTEGER,
                        content_length INTEGER,
                        title TEXT,
                        domain TEXT,
                        metadata TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        crawl_duration REAL
                    )
                ''')
                
                # Create index for faster queries
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON pages(url)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_domain ON pages(domain)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON pages(timestamp)')
                
                # Create table for crawl statistics
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS crawl_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total_pages INTEGER DEFAULT 0,
                        successful_pages INTEGER DEFAULT 0,
                        failed_pages INTEGER DEFAULT 0,
                        total_links_found INTEGER DEFAULT 0,
                        queue_size INTEGER DEFAULT 0,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                conn.commit()
                self.logger.info(f"Database initialized at {self.db_path}")
                
        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise
    
    def save_page(self, url: str, html_content: str, status_code: int, 
                  headers: Dict[str, str], crawl_duration: float) -> bool:
        """
        Save crawled page to database and filesystem
        
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
            
            # Prepare metadata
            metadata = {
                'headers': headers,
                'content_type': headers.get('content-type', ''),
                'server': headers.get('server', ''),
                'last_modified': headers.get('last-modified', ''),
                'content_encoding': headers.get('content-encoding', '')
            }
            
            # Save to database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO pages 
                    (url, html_path, status_code, content_length, title, domain, metadata, crawl_duration)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    url,
                    html_path,
                    status_code,
                    len(html_content),
                    title,
                    domain,
                    json.dumps(metadata),
                    crawl_duration
                ))
                conn.commit()
            
            self.logger.info(f"Saved page: {url} -> {html_path}")
            return True
            
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
    
    def get_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Retrieve page data from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT url, html_path, status_code, content_length, title, 
                           domain, metadata, timestamp, crawl_duration
                    FROM pages WHERE url = ?
                ''', (url,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'url': row[0],
                        'html_path': row[1],
                        'status_code': row[2],
                        'content_length': row[3],
                        'title': row[4],
                        'domain': row[5],
                        'metadata': json.loads(row[6]) if row[6] else {},
                        'timestamp': row[7],
                        'crawl_duration': row[8]
                    }
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving page {url}: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get crawling statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Get page counts
                cursor.execute('SELECT COUNT(*) FROM pages')
                total_pages = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM pages WHERE status_code = 200')
                successful_pages = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM pages WHERE status_code != 200')
                failed_pages = cursor.fetchone()[0]
                
                # Get domain distribution
                cursor.execute('''
                    SELECT domain, COUNT(*) as count 
                    FROM pages 
                    GROUP BY domain 
                    ORDER BY count DESC 
                    LIMIT 10
                ''')
                domain_stats = dict(cursor.fetchall())
                
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
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO crawl_stats 
                    (total_pages, successful_pages, failed_pages, total_links_found, queue_size)
                    VALUES (?, ?, ?, ?, ?)
                ''', (total_pages, successful_pages, failed_pages, total_links, queue_size))
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error saving crawl stats: {e}")
    
    def get_recent_pages(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently crawled pages"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT url, status_code, title, domain, timestamp, crawl_duration
                    FROM pages 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limit,))
                
                pages = []
                for row in cursor.fetchall():
                    pages.append({
                        'url': row[0],
                        'status_code': row[1],
                        'title': row[2],
                        'domain': row[3],
                        'timestamp': row[4],
                        'crawl_duration': row[5]
                    })
                
                return pages
                
        except Exception as e:
            self.logger.error(f"Error getting recent pages: {e}")
            return []
