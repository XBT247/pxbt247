from application.workers.websocket_worker import WebSocketWorker
from core.exchange_factory import ExchangeFactory
from core.interfaces.imessaging import IMessageConsumer
from core.interfaces.iproducer import ITradeProducer
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.repositories.trades_repository import TradesRepository
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
    def _create_repository(cls, config: ExchangeConfig) -> ITradesRepository:
        return TradesRepository(
            db_config=config.database_config,
            exchange=config.exchange_name
        )
    """
    @classmethod
    def _create_websocket_adapter(cls, config: ExchangeConfig) -> IWebSocketAdapter:
        return WebSocketWorker(
            adapter=BinanceWebSocketAdapter(),
            symbols=["btcusdt", "ethusdt"],
            max_reconnect_attempts=5,
            health_check_interval=60
        )"""
    
    @classmethod
    def _create_websocket_adapter(cls, config: ExchangeConfig) -> IWebSocketAdapter:
        # Return raw adapter instead of worker
        return BinanceWebSocketAdapter(
            ws_base_url = config.get_url("ws_base_url")
        )
    
    @classmethod
    def _create_websocket_worker(cls, config: ExchangeConfig) -> WebSocketWorker:
        return WebSocketWorker(
            adapter=BinanceWebSocketAdapter(
                ws_base_url =config.get_url("ws_base_url")
            ),
            symbols=config.highload_pairs,
            max_reconnect_attempts=5,
            health_check_interval=60
        )