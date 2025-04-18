# composition_root/exchange_factory.py
from core.interfaces.imessaging import IMessageConsumer
from infrastructure.config.settings import ExchangeConfig
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.repositories.trades_repository import TradesRepository
from infrastructure.messaging.kafka_consumer import KafkaConsumerClient


class ExchangeFactory:
    @staticmethod
    def create_consumer(exchange: str) -> IMessageConsumer:
        config = ExchangeConfig(exchange)
        return KafkaConsumerClient(
            config.kafka_config['bootstrap_servers'],
            config.kafka_config['raw_topic'],
            f"cgRawTrades-{exchange}"
        )

    @staticmethod
    def create_trade_repo(exchange: str, db_config: dict) -> ITradesRepository:
        return TradesRepository(db_config, exchange)