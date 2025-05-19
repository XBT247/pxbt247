# infra/db/interfaces/trade_repository.py
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from collections import OrderedDict

from infrastructure.db.models.trade_db_model import TradeDBModel

class ITradesRepository(ABC):
    """Interface for trade data operations used by KafkaConsumerBinance"""
    
    @abstractmethod
    async def load_known_tables(self) -> None:
        """Load known trading pair tables into memory"""
        pass

    @abstractmethod
    async def fetch_trades_history(self, symbol: str, lookback_days: int = 30, total_records: int = 0, order_asc: bool = True) -> List[TradeDBModel]:
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
        
    @property
    @abstractmethod
    def batch_timeout(self) -> float:
        """Timeout period for batch flushing in seconds"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Release any repository resources"""
        pass