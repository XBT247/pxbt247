import asyncio
from typing import List
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from core.use_cases.produce_trades import ProduceTradesUseCase

class TradeProducerService:
    def __init__(
        self,
        use_case: ProduceTradesUseCase,
        ws_adapter: IWebSocketAdapter,
        symbols: List[str],
        health_check_interval: int = 30
    ):
        self.use_case = use_case
        self.ws_adapter = ws_adapter
        self.symbols = symbols
        self.health_check_interval = health_check_interval

    async def run(self):
        await self.ws_adapter.connect(self.symbols)
        
        try:
            async for trade in self.ws_adapter.stream_trades():
                await self.use_case.execute(trade)
        finally:
            await self.ws_adapter.disconnect()

    async def health_check(self):
        while True:
            # Implement health checks
            await asyncio.sleep(self.health_check_interval)