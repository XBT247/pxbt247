import asyncio
from typing import List
from application.workers.websocket_worker import WebSocketWorker
from core.use_cases.produce_trades import ProduceTradesUseCase

class TradeProducerService:
    def __init__(
        self,
        use_case: ProduceTradesUseCase,
        ws_worker: WebSocketWorker,
        symbols: List[str],
        health_check_interval: int = 30
    ):
        self.use_case = use_case
        self.ws_worker = ws_worker
        self.symbols = symbols
        self.health_check_interval = health_check_interval

    async def run(self):
        """Main service loop using worker's interface"""
        try:
            # Worker manages its own connection lifecycle
            async for trade in self.ws_worker.run():  # Use worker's main interface
                await self.use_case.execute(trade)
        except Exception as e:
            # Log error and let worker handle reconnection
            await self.ws_worker.shutdown()  # Ensure clean shutdown on error
            raise

    async def health_check(self):
        """Periodic health monitoring"""
        while True:
            # Check worker health status
            if not self.ws_worker.is_healthy():  # Assuming worker has health check
                self.logger.warning("WebSocket worker health check failed")
            await asyncio.sleep(self.health_check_interval)