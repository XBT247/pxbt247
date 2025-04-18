import asyncio
from typing import List
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter

class WebSocketWorker:
    def __init__(
        self,
        adapter: IWebSocketAdapter,
        symbols: List[str],
        max_reconnect_attempts: int = 5
    ):
        self.adapter = adapter
        self.symbols = symbols
        self.max_attempts = max_reconnect_attempts

    async def run(self):
        attempts = 0
        while attempts < self.max_attempts:
            try:
                await self.adapter.connect(self.symbols)
                async for trade in self.adapter.stream_trades():
                    yield trade
                    attempts = 0  # Reset on successful message
            except Exception as e:
                attempts += 1
                wait_time = min(2 ** attempts, 30)  # Exponential backoff
                await asyncio.sleep(wait_time)