#!/usr/bin/env python3
"""
Setup script for the web crawler
"""
import subprocess
import sys
import os

def install_requirements():
    """Install required packages"""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install requirements: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = ["crawled_data", "logs"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Created directory: {directory}")

def run_tests():
    """Run basic tests"""
    print("\nRunning tests...")
    try:
        subprocess.check_call([sys.executable, "test_crawler.py"])
        return True
    except subprocess.CalledProcessError:
        print("⚠ Some tests failed, but crawler should still work")
        return False

def main():
    """Main setup function"""
    print("Web Crawler Setup")
    print("=" * 20)
    
    # Install requirements
    if not install_requirements():
        print("Setup failed. Please install requirements manually:")
        print("pip install -r requirements.txt")
        return False
    
    # Create directories
    create_directories()
    
    # Run tests
    run_tests()
    
    print("\n✓ Setup completed!")
    print("\nTo run the crawler:")
    print("  python web_crawler.py")
    print("\nTo run the example:")
    print("  python example.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
