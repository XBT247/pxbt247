from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

class ICacheRepository(ABC):
    """Interface for cache operations"""
    
    @abstractmethod
    async def get_cached_object(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch cached object from in-memory LRU cache"""
        pass
    
    @abstractmethod
    async def update_cache(self, symbol: str, cached_object: Dict[str, Any]) -> None:
        """Store or update the cache with the new processed object"""
        pass