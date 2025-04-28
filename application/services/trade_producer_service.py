import asyncio
import logging
from typing import List
from application.workers.websocket_worker import WebSocketWorker
from core.use_cases.produce_trades import ProduceTradesUseCase

class TradeProducerService:
    def __init__(
        self,
        use_case: ProduceTradesUseCase,
        ws_worker: WebSocketWorker,
        symbols: List[str],
        health_check_interval: int = 30,
        logger=None  # Added logger parameter
    ):
        self.use_case = use_case
        self.ws_worker = ws_worker
        self.symbols = symbols
        self.health_check_interval = health_check_interval
        self.logger = logger or logging.getLogger(__name__)  # Initialize logger

    async def run(self):
        """Main service loop using worker's interface"""
        try:
            async for trade in self.ws_worker.run():
                try:
                    await self.use_case.execute(trade)
                except Exception as e:
                    self.logger.error(f"Error processing trade: {e}", exc_info=True)
        except asyncio.CancelledError:
            self.logger.info("TradeProducerService run task was cancelled.")
        except Exception as e:
            self.logger.error(f"Error in TradeProducerService: {e}", exc_info=True)
        finally:
            try:
                await self.ws_worker.shutdown()  # Ensure clean shutdown
            except Exception as e:
                self.logger.error(f"Error during WebSocketWorker shutdown: {e}")

    async def health_check(self):
        """Periodic health monitoring"""
        while True:
            # Check worker health status
            if not self.ws_worker.is_healthy():  # Assuming worker has health check
                self.logger.warning("WebSocket worker health check failed")
            await asyncio.sleep(self.health_check_interval)