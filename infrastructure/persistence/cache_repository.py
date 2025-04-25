# cache_repository.py
from collections import OrderedDict
from core.domain.trade import CachedTradeData
from infrastructure.db.interfaces.icache_repository import ICacheRepository

#from core.interfaces.repositories import ICacheRepository

class CacheRepository(ICacheRepository):
    def __init__(self, max_size: int = 5000):
        self.local_cache = OrderedDict()
        self.max_size = max_size

    async def get(self, symbol: str) -> CachedTradeData:
        return self.local_cache.get(symbol.lower(), None)

    async def update(self, symbol: str, data: CachedTradeData):
        symbol = symbol.lower()
        if len(self.local_cache) >= self.max_size:
            self.local_cache.popitem(last=False)
        self.local_cache[symbol] = data
        self.local_cache.move_to_end(symbol)