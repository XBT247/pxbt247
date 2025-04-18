import asyncio
from typing import AsyncIterator
from application.controllers.trade_controller import TradeMessageController
from core.interfaces.imessaging import IMessageConsumer

class TradeConsumerService:
    def __init__(
        self,
        consumer: IMessageConsumer,
        controller: TradeMessageController,
        batch_size: int = 100,
        batch_timeout: float = 5.0
    ):
        self.consumer = consumer
        self.controller = controller
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    async def run(self):
        last_flush_time = asyncio.get_event_loop().time()
        
        async for batch in self._batch_messages():
            for message in batch:
                await self.controller.handle(message)
            
            await self.consumer.commit()
            
            # Periodic flush
            current_time = asyncio.get_event_loop().time()
            if current_time - last_flush_time >= self.batch_timeout:
                last_flush_time = current_time

    async def _batch_messages(self) -> AsyncIterator[list]:
        """Group messages into batches for processing"""
        batch = []
        async for message in self.consumer.consume():
            batch.append(message)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        
        if batch:  # Yield remaining messages
            yield batch