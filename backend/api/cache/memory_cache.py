# api/cache/memory_cache.py
from typing import Any, Optional
import time
import json
from collections import OrderedDict
import threading

class MemoryCache:
    """Thread-safe in-memory cache with TTL and size limits"""
    
    def __init__(self, max_size: int = 1000):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if expiry > time.time():
                    # Move to end (most recently used)
                    self.cache.move_to_end(key)
                    self.hits += 1
                    return value
                else:
                    # Expired
                    del self.cache[key]
            self.misses += 1
            return None
    
    def set(self, key: str, value: Any, ttl: int = 300):
        """Set value with TTL in seconds"""
        with self.lock:
            expiry = time.time() + ttl
            self.cache[key] = (value, expiry)
            self.cache.move_to_end(key)
            
            # Evict oldest if over size limit
            while len(self.cache) > self.max_size:
                self.cache.popitem(last=False)
    
    def delete(self, key: str):
        with self.lock:
            if key in self.cache:
                del self.cache[key]
    
    def clear(self):
        with self.lock:
            self.cache.clear()
    
    def get_stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "size": len(self.cache),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.1f}%"
        }

# Global cache instance
cache = MemoryCache(max_size=2000)

# Cache decorator
def cached(ttl: int = 300):
    """Decorator to cache function results"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # Try to get from cache
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl)
            return result
        return wrapper
    return decorator