from abc import ABC, abstractmethod
from typing import Dict, List, Any

class IBinanceClient(ABC):
    @abstractmethod
    async def get_exchange_info(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def get_account_info(self) -> Dict[str, Any]:
        pass