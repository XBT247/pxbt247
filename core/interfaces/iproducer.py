from abc import ABC, abstractmethod
from core.domain.trade import RawTrade, AggregatedTrade
from core.domain.entities.trading_pair import TradingPair

class ITradeProducer(ABC):
    @abstractmethod
    async def initialize(self):
        pass
        
    @abstractmethod
    async def produce(self, trade: RawTrade):
        pass
        
    @abstractmethod
    async def produce_aggregated(self, trade: AggregatedTrade):
        pass

    @abstractmethod
    async def publish_trading_pair(self, trading_pair: TradingPair) -> bool:
        """Publish trading pair to message bus"""
        pass
    
    @abstractmethod
    async def stop(self):
        pass
