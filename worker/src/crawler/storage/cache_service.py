import redis
from redis.exceptions import RedisError, ConnectionError
from typing import Optional, Set, List
import hashlib
import json
from contextlib import contextmanager

class RedisService:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        decode_responses: bool = True,
        max_connections: int = 50
    ):
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=decode_responses,
            max_connections=max_connections,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        self.client = redis.Redis(connection_pool=self.pool)
        
    def ping(self) -> bool:
        try:
            return self.client.ping()
        except RedisError:
            return False
    
    def close(self):
        self.client.close()
        self.pool.disconnect()
    
    @contextmanager
    def pipeline(self):
        pipe = self.client.pipeline()
        try:
            yield pipe
            pipe.execute()
        except RedisError as e:
            raise e
    
    # URL Content Hash Operations
    def store_url_hash(self, url: str, content: str) -> bool:
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        key = f"url:hash:{url}"
        try:
            self.client.set(key, content_hash)
            return True
        except RedisError:
            return False
    
    def get_url_hash(self, url: str) -> Optional[str]:
        key = f"url:hash:{url}"
        try:
            return self.client.get(key)
        except RedisError:
            return None
    
    def has_url_changed(self, url: str, content: str) -> bool:
        stored_hash = self.get_url_hash(url)
        if not stored_hash:
            return True
        current_hash = hashlib.sha256(content.encode()).hexdigest()
        return stored_hash != current_hash
    
    def delete_url_hash(self, url: str) -> bool:
        key = f"url:hash:{url}"
        try:
            return self.client.delete(key) > 0
        except RedisError:
            return False
    
    def get_all_url_hashes(self) -> dict:
        pattern = "url:hash:*"
        result = {}
        try:
            for key in self.client.scan_iter(match=pattern, count=100):
                url = key.replace("url:hash:", "", 1)
                result[url] = self.client.get(key)
            return result
        except RedisError:
            return {}
    
    # URL Queue Operations (for crawling)
    def enqueue_url(self, queue_name: str, url: str, priority: int = 0) -> bool:
        try:
            self.client.zadd(queue_name, {url: priority})
            return True
        except RedisError:
            return False
    
    def dequeue_url(self, queue_name: str) -> Optional[str]:
        try:
            result = self.client.zpopmin(queue_name, count=1)
            return result[0][0] if result else None
        except RedisError:
            return None
    
    def get_queue_size(self, queue_name: str) -> int:
        try:
            return self.client.zcard(queue_name)
        except RedisError:
            return 0
    
    # URL Set Operations (visited/seen tracking)
    def add_to_set(self, set_name: str, url: str) -> bool:
        try:
            return self.client.sadd(set_name, url) > 0
        except RedisError:
            return False
    
    def is_in_set(self, set_name: str, url: str) -> bool:
        try:
            return self.client.sismember(set_name, url)
        except RedisError:
            return False
    
    def remove_from_set(self, set_name: str, url: str) -> bool:
        try:
            return self.client.srem(set_name, url) > 0
        except RedisError:
            return False
    
    def get_set_members(self, set_name: str) -> Set[str]:
        try:
            return self.client.smembers(set_name)
        except RedisError:
            return set()
    
    def get_set_size(self, set_name: str) -> int:
        try:
            return self.client.scard(set_name)
        except RedisError:
            return 0
    
    # URL Metadata Operations
    def store_url_metadata(self, url: str, metadata: dict) -> bool:
        key = f"url:meta:{url}"
        try:
            self.client.hset(key, mapping=metadata)
            return True
        except RedisError:
            return False
    
    def get_url_metadata(self, url: str) -> dict:
        key = f"url:meta:{url}"
        try:
            return self.client.hgetall(key)
        except RedisError:
            return {}
    
    def update_url_field(self, url: str, field: str, value: str) -> bool:
        key = f"url:meta:{url}"
        try:
            self.client.hset(key, field, value)
            return True
        except RedisError:
            return False
    
    def get_url_field(self, url: str, field: str) -> Optional[str]:
        key = f"url:meta:{url}"
        try:
            return self.client.hget(key, field)
        except RedisError:
            return None
    
    # Expiration Operations
    def set_expiration(self, key: str, seconds: int) -> bool:
        try:
            return self.client.expire(key, seconds)
        except RedisError:
            return False
    
    def get_ttl(self, key: str) -> int:
        try:
            return self.client.ttl(key)
        except RedisError:
            return -2
    
    # Batch Operations
    def batch_store_url_hashes(self, url_content_pairs: List[tuple]) -> int:
        success_count = 0
        try:
            with self.pipeline() as pipe:
                for url, content in url_content_pairs:
                    content_hash = hashlib.sha256(content.encode()).hexdigest()
                    key = f"url:hash:{url}"
                    pipe.set(key, content_hash)
            success_count = len(url_content_pairs)
        except RedisError:
            pass
        return success_count
    
    def batch_check_urls_exist(self, urls: List[str]) -> dict:
        result = {}
        try:
            with self.client.pipeline() as pipe:
                keys = [f"url:hash:{url}" for url in urls]
                for key in keys:
                    pipe.exists(key)
                results = pipe.execute()
                for url, exists in zip(urls, results):
                    result[url] = bool(exists)
        except RedisError:
            pass
        return result
    
    # Key Management
    def delete_keys_by_pattern(self, pattern: str) -> int:
        deleted = 0
        try:
            for key in self.client.scan_iter(match=pattern, count=100):
                deleted += self.client.delete(key)
        except RedisError:
            pass
        return deleted
    
    def key_exists(self, key: str) -> bool:
        try:
            return self.client.exists(key) > 0
        except RedisError:
            return False
    
    def flush_db(self) -> bool:
        try:
            self.client.flushdb()
            return True
        except RedisError:
            return False
    
    # Statistics
    def get_db_size(self) -> int:
        try:
            return self.client.dbsize()
        except RedisError:
            return 0
    
    def get_info(self, section: Optional[str] = None) -> dict:
        try:
            return self.client.info(section)
        except RedisError:
            return {}


# Usage Example
if __name__ == "__main__":
    redis_service = RedisService(host="redis", port=6379)
    
    if redis_service.ping():
        url = "https://example.com"
        content = "<html>Example content</html>"
        
        redis_service.store_url_hash(url, content)
        stored_hash = redis_service.get_url_hash(url)
        print(f"Stored hash: {stored_hash}")
        
        changed = redis_service.has_url_changed(url, content + " modified")
        print(f"Content changed: {changed}")
        
        redis_service.close()
    else:
        print("No Response")