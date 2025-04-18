from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class ITradingPairsRepository(ABC):
    @abstractmethod
    async def save_trading_pair(self, pair_data: Dict[str, Any]) -> bool:
        pass
        
    @abstractmethod
    async def fetch_trading_pairs(self) -> Dict[str, Dict[str, str]]:
        pass
        
    @abstractmethod
    async def ensure_table_exists(self, symbol: str) -> bool:
        pass