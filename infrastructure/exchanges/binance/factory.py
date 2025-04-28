from application.workers.websocket_worker import WebSocketWorker
from core import logging
from core.exchange_factory import ExchangeFactory
from typing import List
from core.interfaces.imessaging import IMessageConsumer
from core.interfaces.iproducer import ITradeProducer
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from infrastructure.db.repositories.trades_repository import TradesRepository
from infrastructure.db.repositories.tradingpairs_repository import TradingPairsRepository
from infrastructure.messaging.kafka_producer import KafkaProducerClient
from infrastructure.exchanges.binance.adapter import BinanceWebSocketAdapter
from infrastructure.persistence.trade_repository import TradeRepository
from core.domain.exchange import ExchangeConfig

class BinanceExchangeFactory(ExchangeFactory, exchange="binance"):
    @classmethod
    def _create_producer(cls, config: ExchangeConfig) -> ITradeProducer:
        return KafkaProducerClient(
            bootstrap_servers=config.kafka_bootstrap_servers,
            raw_topic=config.get_topic("trades_raw"),
            agg_topic=config.get_topic("trades_agg")
        )
    
    @classmethod
    def _create_consumer(cls, config: ExchangeConfig) -> IMessageConsumer:
        from infrastructure.messaging.kafka_consumer import KafkaConsumerClient
        return KafkaConsumerClient(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic=config.get_topic("trades_raw"),
            group_id=f"cgRawTrades-{config.exchange_name}"
        )
    
    @classmethod
    def _create_repository_trades(cls, config: ExchangeConfig, logger: logging.Logger = None) -> ITradesRepository:
        return TradesRepository(
            db_config=config.database_config,
            exchange=config.exchange_name,
            logger=logger
        )
    
    @classmethod
    def _create_repository_tradingpairs(cls, config: ExchangeConfig, logger: logging.Logger = None) -> ITradingPairsRepository:
        return TradingPairsRepository(
            db_config=config.database_config,
            exchange=config.exchange_name,
            logger=logger
        )

    @classmethod
    def _create_websocket_adapter(cls, config: ExchangeConfig, logger: logging.Logger = None) -> IWebSocketAdapter:
        return BinanceWebSocketAdapter(
            ws_base_url=config.get_url("ws_base_url"),
            logger=logger
        )
    
    @classmethod
    def _create_websocket_worker(cls, config: ExchangeConfig, symbols: List[str] = None, logger: logging.Logger = None) -> WebSocketWorker:
        return WebSocketWorker(
            adapter=cls._create_websocket_adapter(config, logger),
            symbols=symbols or config.highload_pairs,
            logger=logger,  # Pass logger to WebSocketWorker
            max_reconnect_attempts=5,
            health_check_interval=60,
            max_streams_per_connection=50
        )