from abc import ABC, abstractmethod
from typing import List
from core.domain.entities.trading_pair import TradingPair

class ITradingPairsRepository(ABC):
    @abstractmethod
    async def save_trading_pair(self, pair_data: TradingPair) -> bool:
        pass
        
    @abstractmethod
    async def fetch_trading_pairs(self) -> List[TradingPair]:
        pass
        
    @abstractmethod
    async def fetch_slots_data(self, symbol: str) -> List[TradingPair]:
        pass
    
    @abstractmethod
    async def ensure_table_exists(self, symbol: str) -> bool:
        pass
    @abstractmethod
    async def update_slots_data(self, symbol: str, volume_slots: str, trade_slots: str, stats: str) -> bool:
        pass

    @abstractmethod
    async def close(self) -> None:
        """Release any repository resources"""
        pass