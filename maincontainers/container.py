import asyncio
import logging
from logging.config import dictConfig
from typing import Dict
from application.controllers.trade_controller import TradeMessageController
from core.exchange_factory import ExchangeFactory
from core.domain.exchange import ExchangeConfig
from core.use_cases.process_trades import ProcessTradeUseCase
from core.use_cases.produce_trades import ProduceTradesUseCase
from infrastructure.config.settings import load_config
from infrastructure.db.repositories.cache_repository import CacheRepository
from infrastructure.exchanges.binance.factory import BinanceExchangeFactory
from application.services.trade_consumer_service import TradeConsumerService
from application.services.trade_producer_service import TradeProducerService
from infrastructure.messaging.kafka_producer import KafkaProducerClient
from core.logging import Logger


class Container:
    def __init__(self, exchange: str):
        self.exchange = exchange
        config_data = load_config(exchange)
        self.config = ExchangeConfig(exchange, config_data)
        self.logger = Logger.get_logger(f"container.{exchange}")
        # Initialize all attributes to None
        self.producer = None
        self.ws_adapter = None
        self.consumer = None
        self.repository = None
        self.producer_service = None
        self.consumer_service = None
        
    async def initialize(self):
        try:
            # Initialize producer components
            self.producer = ExchangeFactory.create_producer(self.exchange, self.config)
            self.ws_worker = ExchangeFactory.create_websocket_worker(
                self.exchange,
                self.config
            )            
            # Initialize consumer components
            self.consumer = ExchangeFactory.create_consumer(self.exchange, self.config)
            self.repository = ExchangeFactory.create_repository(self.exchange, self.config)

            cache_repo = CacheRepository()  # Or your actual cache implementation
            trend_producer = ExchangeFactory.create_producer(self.exchange, self.config) #topic="cache.trendaware"
            
            # Initialize services
            #from application.services import TradeProducerService, TradeConsumerService
            self.producer_service = TradeProducerService(
                use_case=ProduceTradesUseCase(
                    producer=self.producer,
                    repository=self.repository ,
                    aggregation_window=5  # Optional: default is 5 seconds
                ),
                ws_worker=self.ws_worker,
                symbols=self.config.highload_pairs # Get symbols from config
            )
            self.consumer_service = TradeConsumerService(
                consumer=self.consumer,
                controller=TradeMessageController(
                    use_case=ProcessTradeUseCase(
                        trade_repo=self.repository,
                        cache_repo=cache_repo,
                        producer=trend_producer
                    )
                ),
                batch_size=self.config.get('batch_size', 100),
                batch_timeout=self.config.get('batch_timeout', 5.0)
            )
        except Exception as e:
            self.logger.error(f"Container Initialization failed: {e}")
            await self.shutdown()
            raise
    
    async def shutdown(self):
        shutdown_tasks = []        
        # Standard components
        if hasattr(self, 'producer') and self.producer:
            shutdown_tasks.append(self.producer.stop())
        if hasattr(self, 'ws_adapter') and self.ws_adapter:
            shutdown_tasks.append(self.ws_adapter.shutdown())
        if hasattr(self, 'consumer') and self.consumer:
            shutdown_tasks.append(self.consumer.stop())        
        # Repository cleanup
        if hasattr(self, 'repository') and self.repository and hasattr(self.repository, 'close'):
            shutdown_tasks.append(self.repository.close())
        
        await asyncio.gather(*shutdown_tasks)

