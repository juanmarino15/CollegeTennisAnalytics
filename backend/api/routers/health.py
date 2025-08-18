# api/routers/health.py
from fastapi import APIRouter
from api.cache.memory_cache import cache
from api.database import engine

router = APIRouter()

@router.get("/health/cache")
def cache_stats():
    """Get cache statistics"""
    return {
        "cache_stats": cache.get_stats(),
        "db_pool": {
            "size": engine.pool.size(),
            "checked_out": engine.pool.checkedout(),
            "overflow": engine.pool.overflow(),
            "total": engine.pool.checkedout() + engine.pool.checkedin()
        }
    }

@router.delete("/health/cache")
def clear_cache():
    """Clear the cache (admin only)"""
    cache.clear()
    return {"message": "Cache cleared"}