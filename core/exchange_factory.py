from typing import Dict, Type, List
import uuid
from application.workers.websocket_worker import WebSocketWorker
from core import logging
from core.domain.exchange import ExchangeConfig
from core.interfaces.imessaging import IMessageConsumer
from core.interfaces.iproducer import ITradeProducer
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from infrastructure.exchanges.binance.adapter import BinanceWebSocketAdapter
from infrastructure.messaging.kafka_consumer import KafkaConsumerClient
from infrastructure.messaging.kafka_producer import KafkaProducerClient
from core.logging import Logger

Logger.configure()  # Called once during app startup

class ExchangeFactory:
    _registry: Dict[str, Type['ExchangeFactory']] = {}
    
    def __init_subclass__(self, exchange: str, **kwargs):
        super().__init_subclass__(**kwargs)
        self._registry[exchange] = self
        self.logger = Logger.get_logger(f"factory.{self.__name__.lower()}")

    @classmethod
    def _validate_kafka_config(self, config: ExchangeConfig):
        required = ['kafka_bootstrap_servers', 'trades_raw']
        if not all(config.get(k) for k in required):
            raise ValueError(f"Missing required Kafka config: {required}")
        
    @classmethod
    async def shutdown_producer(self, producer: ITradeProducer):
        if producer:
            await producer.stop()

    @classmethod
    async def shutdown_consumer(self, consumer: IMessageConsumer):
        if consumer:
            await consumer.stop()

    @classmethod
    def create_producer(self, exchange: str, config: ExchangeConfig, logger: logging.Logger = None) -> ITradeProducer:
        factory = self._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_producer(config)
    
    @classmethod
    def create_consumer(self, exchange: str, config: ExchangeConfig) -> IMessageConsumer:
        factory = self._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_consumer(config)
    
    @classmethod
    def create_repository_trades(self, exchange: str, config: ExchangeConfig, logger: logging.Logger = None) -> ITradesRepository:
        factory = self._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_repository_trades(config, logger)
    
    
    @classmethod
    def create_repository_tradingpairs(self, exchange: str, config: ExchangeConfig, logger: logging.Logger = None) -> ITradingPairsRepository:
        factory = self._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_repository_tradingpairs(config, logger)
        
    @classmethod
    def create_websocket_worker(cls, exchange: str, config: ExchangeConfig, symbols: List[str] = None, logger: logging.Logger = None) -> WebSocketWorker:
        factory = cls._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_websocket_worker(config, symbols, logger)
    
    @classmethod
    def _create_producer(self, config: ExchangeConfig) -> ITradeProducer:
        try:
            producer = KafkaProducerClient(
                bootstrap_servers=config.kafka_bootstrap_servers,
                raw_topic=config.get_topic("trades_raw"),
                agg_topic=config.get_topic("trades_agg"),
                linger_ms=config.get("kafka_linger_ms", 100),
                batch_size=config.get("kafka_batch_size", 16384),
                max_retries=3  # New parameter
            )
            self.logger.info(f"Created producer for {config.exchange_name}")
            return producer
        except Exception as e:
            self.logger.error(f"Failed to create producer: {e}")
            raise

    @classmethod
    def _create_consumer(self, config: ExchangeConfig) -> IMessageConsumer:
        try:
            consumer = KafkaConsumerClient(
                bootstrap_servers=config.kafka_bootstrap_servers,
                topic=config.get_topic('trades_raw'),
                group_id=f"cg-{config.exchange_name}-{uuid.uuid4().hex[:8]}",
                session_timeout_ms=config.get("kafka_session_timeout", 15000),
                heartbeat_interval_ms=config.get("kafka_heartbeat_interval", 5000),
                max_poll_interval_ms=300000  # New: 5min timeout for processing
            )
            self.logger.info(f"Created consumer for {config.exchange_name}")
            return consumer
        except Exception as e:
            self.logger.error(f"Failed to create consumer: {e}")
            raise
        
    @classmethod
    def _create_repository_trades(self, config: ExchangeConfig) -> ITradesRepository:
        raise NotImplementedError
    
     
    @classmethod
    def _create_repository_tradingpairs(self, config: ExchangeConfig) -> ITradingPairsRepository:
        raise NotImplementedError
    
    @classmethod
    def _create_websocket_adapter(self, config: ExchangeConfig) -> IWebSocketAdapter:
        return BinanceWebSocketAdapter(
            ws_base_url=config.get_url("ws_base_url"),
            logger=Logger.get_logger("BinanceWebSocketAdapter")
        )