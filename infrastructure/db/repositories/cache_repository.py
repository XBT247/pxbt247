import json
from typing import Dict, List, Any, Optional
from collections import OrderedDict, defaultdict
from infrastructure.db.interfaces.icache_repository import ICacheRepository

class CacheRepository(ICacheRepository):
    """In-memory cache repository implementation"""
    
    def __init__(self, max_size: int = 5000):
        self.local_cache = OrderedDict()
        self.max_size = max_size
        
    async def get_cached_object(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch cached object from in-memory LRU cache"""
        return self.local_cache.get(symbol.lower(), None)
        
    async def update_cache(self, symbol: str, cached_object: Dict[str, Any]) -> None:
        """Store or update the cache with the new processed object"""
        symbol = symbol.lower()
        if len(self.local_cache) >= self.max_size:
            self.local_cache.popitem(last=False)
        self.local_cache[symbol] = cached_object
        self.local_cache.move_to_end(symbol)