# application/services/multi_exchange_service.py
import asyncio
from application.services.trade_consumer_service import TradeConsumerService


class MultiExchangeConsumer:
    def __init__(self, exchanges: list[str]):
        self.exchanges = exchanges
        self.services = {}
        
        for exchange in exchanges:
            consumer = ExchangeFactory.create_consumer(exchange)
            repo = ExchangeFactory.create_trade_repo(exchange, ...)
            # ... build other dependencies
            self.services[exchange] = TradeConsumerService(consumer, ...)

    async def run_all(self):
        await asyncio.gather(*[
            service.run() for service in self.services.values()
        ])