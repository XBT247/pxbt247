# infra/db/interfaces/trade_repository.py
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from collections import OrderedDict

class ITradesRepository(ABC):
    """Interface for trade data operations used by KafkaConsumerBinance"""
    
    @abstractmethod
    async def load_known_tables(self) -> None:
        """Load known trading pair tables into memory"""
        pass
    
    @abstractmethod
    async def add_to_batch(self, symbol: str, trade_data: Dict[str, Any]) -> None:
        """
        Add trade data to batch for later insertion
        Args:
            symbol: Trading symbol (e.g., 'btcusdt')
            trade_data: Dictionary containing trade data
        """
        pass
    
    @abstractmethod
    async def flush_batches(self) -> None:
        """Flush all batched trade data to database"""
        pass
    
    @abstractmethod
    async def start_periodic_flusher(self) -> None:
        """Start background task for periodic batch flushing"""
        pass
    
    @abstractmethod
    async def close_pool(self) -> None:
        """Close database connection pool"""
        pass
    
    @property
    @abstractmethod
    def batch_timeout(self) -> float:
        """Timeout period for batch flushing in seconds"""
        pass
