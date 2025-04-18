from abc import ABC, abstractmethod
from typing import AsyncIterator

class IMessageConsumer(ABC):
    @abstractmethod
    async def consume(self) -> AsyncIterator[dict]:
        pass

    @abstractmethod
    async def commit(self):
        pass

    @abstractmethod
    async def stop(self):
        pass

class IMessageProducer(ABC):
    @abstractmethod
    async def send(self, topic: str, key: str, value: dict):
        pass

    @abstractmethod
    async def stop(self):
        pass