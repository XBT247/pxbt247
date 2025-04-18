from abc import ABC, abstractmethod
from core.domain.trade import RawTrade
from typing import AsyncIterator

class IWebSocketAdapter(ABC):
    @abstractmethod
    async def connect(self, symbols: list[str]):
        pass
        
    @abstractmethod
    async def disconnect(self):
        pass
        
    @abstractmethod
    async def stream_trades(self) -> AsyncIterator[RawTrade]:
        pass