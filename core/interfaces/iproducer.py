from abc import ABC, abstractmethod
from core.domain.trade import RawTrade, AggregatedTrade

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
    async def stop(self):
        pass
