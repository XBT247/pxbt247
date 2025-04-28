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
        self.ws_worker = None
        self.consumer = None
        self.repository_trades = None
        self.repository_tradingpairs = None
        self.producer_service = None
        self.consumer_service = None
        
    async def initialize(self):
        try:
            # Initialize producer components
            self.producer = ExchangeFactory.create_producer(self.exchange, self.config, logger=self.logger)
                     
            # Initialize consumer components
            self.consumer = ExchangeFactory.create_consumer(self.exchange, self.config)
            self.repository_trades = ExchangeFactory.create_repository_trades(self.exchange, self.config, logger=self.logger)
            self.repository_tradingpairs = ExchangeFactory.create_repository_tradingpairs(self.exchange, self.config, logger=self.logger)

            cache_repo = CacheRepository()  # Or your actual cache implementation
            trend_producer = ExchangeFactory.create_producer(self.exchange, self.config, logger=self.logger) #topic="cache.trendaware"
            
            # Fetch trading pairs and combine with config.highload_pairs
            fetched_pairs = await self.repository_tradingpairs.fetch_trading_pairs()
            combined_pairs = self.config.highload_pairs + list(fetched_pairs.keys())
            self.ws_worker = ExchangeFactory.create_websocket_worker(
                self.exchange,
                self.config,
                combined_pairs,
                logger=self.logger
            )   
            # Initialize services
            self.producer_service = TradeProducerService(
                use_case=ProduceTradesUseCase(
                    producer=self.producer,
                    repository=self.repository_trades ,
                    aggregation_window=5  # Optional: default is 5 seconds
                ),
                ws_worker=self.ws_worker,
                symbols=combined_pairs 
            )
            
            self.consumer_service = TradeConsumerService(
                consumer=self.consumer,
                controller=TradeMessageController(
                    use_case=ProcessTradeUseCase(
                        trade_repo=self.repository_trades,
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
        if hasattr(self, 'ws_worker') and self.ws_worker:
            shutdown_tasks.append(self.ws_worker.shutdown())
        if hasattr(self, 'consumer') and self.consumer:
            shutdown_tasks.append(self.consumer.stop())
        # Repository cleanup
        if hasattr(self, 'repository_trades') and self.repository_trades and hasattr(self.repository_trades, 'close'):
            shutdown_tasks.append(self.repository_trades.close())

        try:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

