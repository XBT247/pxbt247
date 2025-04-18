from typing import Dict
from application.services.trade_consumer_service import TradeConsumerService
from application.services.trade_producer_service import TradeProducerService
from core.exchange_factory import ExchangeFactory
from core.domain.exchange import ExchangeConfig
from infrastructure.config.settings import load_config
#from config.settings import load_config

class Container:
    def __init__(self, exchange: str):
        self.exchange = exchange
        self.config = ExchangeConfig(exchange, load_config(exchange))
        
    async def initialize(self):
        # Initialize producer components
        self.producer = ExchangeFactory.create_producer(self.exchange, self.config)
        self.ws_adapter = ExchangeFactory.create_websocket_adapter(self.exchange, self.config)
        
        # Initialize consumer components
        self.consumer = ExchangeFactory.create_consumer(self.exchange, self.config)
        self.repository = ExchangeFactory.create_repository(self.exchange, self.config)
        
        # Initialize services
        #from application.services import TradeProducerService, TradeConsumerService
        self.producer_service = TradeProducerService(
            producer=self.producer,
            ws_adapter=self.ws_adapter,
            config=self.config
        )
        
        self.consumer_service = TradeConsumerService(
            consumer=self.consumer,
            repository=self.repository,
            config=self.config
        )
    
    async def shutdown(self):
        await self.producer.stop()
        await self.ws_adapter.disconnect()
        await self.consumer.stop()
        await self.repository.close_pool()