#!/usr/bin/env python3
"""
Test script for the web crawler
"""
import sys
import os

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all modules can be imported"""
    try:
        from url_frontier import URLFrontier
        from html_downloader import HTMLDownloader
        from robots_parser import RobotsParser
        from link_extractor import LinkExtractor
        from url_seen import URLSeen
        from content_storage import ContentStorage
        from logger import CrawlerLogger
        from web_crawler import WebCrawler
        print("✓ All modules imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of components"""
    try:
        # Test URL Frontier
        frontier = URLFrontier(max_size=10)
        assert frontier.add_url("https://example.com")
        assert frontier.size() == 1
        assert not frontier.is_empty()
        url = frontier.get_url()
        assert url == "https://example.com"
        assert frontier.is_empty()
        print("✓ URL Frontier working")
        
        # Test HTML Downloader
        downloader = HTMLDownloader(timeout=5)
        assert downloader.is_valid_url("https://example.com")
        assert not downloader.is_valid_url("invalid-url")
        print("✓ HTML Downloader working")
        
        # Test Robots Parser
        robots = RobotsParser()
        assert robots.is_allowed("https://example.com")
        print("✓ Robots Parser working")
        
        # Test Link Extractor
        extractor = LinkExtractor()
        html = '<html><body><a href="https://example.com">Link</a></body></html>'
        links = extractor.extract_links(html, "https://test.com")
        assert "https://example.com" in links
        print("✓ Link Extractor working")
        
        # Test URL Seen (without Bloom filter for testing)
        try:
            from url_seen import URLSeen
            seen = URLSeen(capacity=100)
            assert seen.add_url("https://example.com")
            assert seen.has_seen("https://example.com")
            assert not seen.has_seen("https://other.com")
            print("✓ URL Seen working")
        except ImportError:
            print("⚠ URL Seen requires pybloom-live (install with: pip install pybloom-live)")
        
        # Test Content Storage
        storage = ContentStorage(db_path="test.db", output_dir="test_output")
        stats = storage.get_stats()
        assert isinstance(stats, dict)
        print("✓ Content Storage working")
        
        # Test Logger
        logger = CrawlerLogger(stats_interval=1)
        logger.log_info("Test message")
        stats = logger.get_stats()
        assert isinstance(stats, dict)
        print("✓ Logger working")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("Testing Web Crawler Components...")
    print("=" * 40)
    
    # Test imports
    if not test_imports():
        print("\n❌ Import tests failed. Please install dependencies:")
        print("pip install -r requirements.txt")
        return False
    
    print()
    
    # Test basic functionality
    if not test_basic_functionality():
        print("\n❌ Functionality tests failed")
        return False
    
    print("\n✅ All tests passed!")
    print("\nTo run the crawler:")
    print("python web_crawler.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
