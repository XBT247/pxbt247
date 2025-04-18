from typing import Dict, Type
from core.domain.exchange import ExchangeConfig
from core.interfaces.imessaging import IMessageConsumer
from core.interfaces.iproducer import ITradeProducer
from infrastructure.db.interfaces.itrade_repository import ITradesRepository

class ExchangeFactory:
    _registry: Dict[str, Type['ExchangeFactory']] = {}
    
    def __init_subclass__(cls, exchange: str, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry[exchange] = cls
        
    @classmethod
    def create_producer(cls, exchange: str, config: ExchangeConfig) -> ITradeProducer:
        factory = cls._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_producer(config)
    
    @classmethod
    def create_consumer(cls, exchange: str, config: ExchangeConfig) -> IMessageConsumer:
        factory = cls._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_consumer(config)
    
    @classmethod
    def create_repository(cls, exchange: str, config: ExchangeConfig) -> ITradesRepository:
        factory = cls._registry.get(exchange)
        if not factory:
            raise ValueError(f"No factory registered for exchange: {exchange}")
        return factory._create_repository(config)
    
    @classmethod
    def _create_producer(cls, config: ExchangeConfig) -> ITradeProducer:
        raise NotImplementedError
        
    @classmethod
    def _create_consumer(cls, config: ExchangeConfig) -> IMessageConsumer:
        raise NotImplementedError
        
    @classmethod
    def _create_repository(cls, config: ExchangeConfig) -> ITradesRepository:
        raise NotImplementedError