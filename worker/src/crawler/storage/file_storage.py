import os
import re
import hashlib
import requests
import boto3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import List
from abc import ABC, abstractmethod
from typing import Optional, List
from pathlib import Path
from botocore.exceptions import ClientError


class StorageBackend(ABC):
    @abstractmethod
    def create(self, path: str, content: bytes) -> bool:
        pass
    
    @abstractmethod
    def read(self, path: str) -> Optional[bytes]:
        pass
    
    @abstractmethod
    def update(self, path: str, content: bytes) -> bool:
        pass
    
    @abstractmethod
    def delete(self, path: str) -> bool:
        pass
    
    @abstractmethod
    def list(self, prefix: str = "") -> List[str]:
        pass


class BlockStorage(StorageBackend):
    def __init__(self, base_path: str = "./storage"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_full_path(self, path: str) -> Path:
        return self.base_path / path
    
    def create(self, path: str, content: bytes) -> bool:
        full_path = self._get_full_path(path)
        if full_path.exists():
            return False
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(content)
        return True
    
    def read(self, path: str) -> Optional[bytes]:
        full_path = self._get_full_path(path)
        if not full_path.exists():
            return None
        return full_path.read_bytes()
    
    def update(self, path: str, content: bytes) -> bool:
        full_path = self._get_full_path(path)
        if not full_path.exists():
            return False
        full_path.write_bytes(content)
        return True
    
    def delete(self, path: str) -> bool:
        full_path = self._get_full_path(path)
        if not full_path.exists():
            return False
        full_path.unlink()
        return True
    
    def list(self, prefix: str = "") -> List[str]:
        search_path = self.base_path / prefix if prefix else self.base_path
        if not search_path.exists():
            return []
        files = []
        for item in search_path.rglob("*"):
            if item.is_file():
                files.append(str(item.relative_to(self.base_path)))
        return files


class S3Storage(StorageBackend):
    def __init__(self, bucket: str, region: str = "us-east-1", 
                 aws_access_key: Optional[str] = None,
                 aws_secret_key: Optional[str] = None):
        self.bucket = bucket
        if aws_access_key and aws_secret_key:
            self.s3 = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            self.s3 = boto3.client('s3', region_name=region)
        
        self._verify_connection()
    
    def _verify_connection(self) -> None:
        """Verify S3 connection and bucket access"""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise ConnectionError(f"Bucket '{self.bucket}' does not exist")
            elif error_code == '403':
                raise ConnectionError(f"Access denied to bucket '{self.bucket}'")
            else:
                raise ConnectionError(f"Failed to connect to S3: {str(e)}")
    
    
    def create(self, path: str, content: bytes) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return False
        except ClientError:
            pass
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=content)
        return True
    
    def read(self, path: str) -> Optional[bytes]:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=path)
            return response['Body'].read()
        except ClientError:
            return None
    
    def update(self, path: str, content: bytes) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
        except ClientError:
            return False
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=content)
        return True
    
    def delete(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            self.s3.delete_object(Bucket=self.bucket, Key=path)
            return True
        except ClientError:
            return False
    
    def list(self, prefix: str = "") -> List[str]:
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents']]
        except ClientError:
            return []


class FileService:
    def __init__(self, backend: StorageBackend):
        self.backend = backend
    
    def upload_file(self, source_path: str, dest_path: Optional[str] = None) -> bool:
        """Upload file from local filesystem to storage backend"""
        source = Path(source_path)
        if not source.exists() or not source.is_file():
            return False
        
        target = dest_path if dest_path else source.name
        content = source.read_bytes()
        return self.backend.create(target, content)

    def upload_directory(self, dir_path: str, prefix: str = "", recursive: bool = True) -> dict:
        """Upload entire directory to storage backend"""
        source_dir = Path(dir_path)
        if not source_dir.exists() or not source_dir.is_dir():
            return {"success": False, "uploaded": [], "failed": []}
        
        uploaded = []
        failed = []
        
        if recursive:
            items = source_dir.rglob("*")
        else:
            items = source_dir.glob("*")
        
        for item in items:
            if item.is_file():
                relative = item.relative_to(source_dir)
                dest = str(Path(prefix) / relative) if prefix else str(relative)
                dest = dest.replace("\\", "/")
                
                try:
                    content = item.read_bytes()
                    if self.backend.create(dest, content):
                        uploaded.append(dest)
                    else:
                        failed.append(dest)
                except Exception:
                    failed.append(dest)
        
        return {"success": len(failed) == 0, "uploaded": uploaded, "failed": failed}
    
    def create_file(self, path: str, content: bytes) -> bool:
        return self.backend.create(path, content)
    
    def read_file(self, path: str) -> Optional[bytes]:
        return self.backend.read(path)
    
    def update_file(self, path: str, content: bytes) -> bool:
        return self.backend.update(path, content)
    
    def delete_file(self, path: str) -> bool:
        return self.backend.delete(path)
    
    def list_files(self, prefix: str = "") -> List[str]:
        return self.backend.list(prefix)

'''
# Usage examples
if __name__ == "__main__":
    # Block storage
    block = BlockStorage("./data")
    fs_block = FileService(block)
    
    # Upload file - just provide source path
    fs_block.upload_file("/path/to/local/file.txt")
    
    # Or specify destination path
    fs_block.upload_file("/path/to/local/file.txt", "documents/renamed.txt")
    
    # Other operations
    content = fs_block.read_file("file.txt")
    print(f"Read: {content}")
    
    fs_block.update_file("file.txt", b"Updated content")
    fs_block.delete_file("file.txt")
    
    # S3 storage
    # s3 = S3Storage(bucket="my-bucket", region="us-east-1")
    # fs_s3 = FileService(s3)
    # fs_s3.upload_file("/path/to/local/file.txt", "documents/file.txt")
'''


def extract_property_id(url: str) -> str:
    """
    Craigslist property id is usually the last numeric token in URL
    """
    match = re.search(r"/(\d+)\.html", url)
    if not match:
        raise ValueError(f"Could not extract property_id from {url}")
    return match.group(1)


def ensure_dirs(base_path: str) -> str:
    images_path = os.path.join(base_path, "images")
    os.makedirs(images_path, exist_ok=True)
    return images_path


def save_html(base_path: str, html: str) -> None:
    html_path = os.path.join(base_path, "index.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)


def extract_image_urls(html: str, page_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []

    for img in soup.find_all("img"):
        src = img.get("src")
        if not src:
            continue
        urls.append(urljoin(page_url, src))

    return list(set(urls))


def download_images(image_urls: List[str], images_path: str) -> None:
    for url in image_urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            ext = os.path.splitext(urlparse(url).path)[-1] or ".jpg"
            name = hashlib.sha256(url.encode()).hexdigest()[:16] + ext

            with open(os.path.join(images_path, name), "wb") as f:
                f.write(response.content)

        except Exception:
            # Intentionally swallow image errors
            continue
