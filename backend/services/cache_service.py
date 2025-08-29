# services/cache_service.py
import redis.asyncio as redis
import json
import os
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)

class CacheService:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client = None
    
    async def initialize(self):
        """Initialize Redis connection"""
        try:
            self.client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            await self.client.ping()
            logger.info("Redis cache service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            # For prototype, continue without cache
            self.client = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self.client:
            return None
        
        try:
            value = await self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, expire: Optional[int] = None):
        """Set value in cache with optional expiration"""
        if not self.client:
            return False
        
        try:
            serialized_value = json.dumps(value, default=str)
            if expire:
                await self.client.setex(key, expire, serialized_value)
            else:
                await self.client.set(key, serialized_value)
            return True
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str):
        """Delete key from cache"""
        if not self.client:
            return False
        
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def health_check(self) -> str:
        """Check cache service health"""
        if not self.client:
            return "disconnected"
        
        try:
            await self.client.ping()
            return "healthy"
        except Exception:
            return "error"
    
    async def close(self):
        """Close Redis connection"""
        if self.client:
            await self.client.close()