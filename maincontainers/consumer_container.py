from core.use_cases.process_trades import ProcessTradeUseCase
from application.controllers.trade_controller import TradeMessageController
from infrastructure.config.settings import load_config
from infrastructure.messaging.kafka_consumer import KafkaConsumerClient
from infrastructure.messaging.kafka_producer import KafkaProducerClient
from infrastructure.persistence.cache_repository import CacheRepository
from infrastructure.persistence.trade_repository import TradeRepository

class ConsumerContainer:
    def __init__(self, config):
        self.config = config
        
    async def init_resources(self):
        # Initialize all dependencies
        self.trade_repo = TradeRepository(self.config['database'])
        self.cache_repo = CacheRepository()
        
        self.consumer = KafkaConsumerClient(
            self.config['kafka']['bootstrap_servers'],
            self.config['kafka']['topic'],
            self.config['kafka']['group_id']
        )
        
        self.producer = KafkaProducerClient(
            self.config['kafka']['bootstrap_servers']
        )
        
        self.process_use_case = ProcessTradeUseCase(
            self.trade_repo,
            self.cache_repo
        )
        
        self.controller = TradeMessageController(
            self.process_use_case
        )

async def main():
    config = load_config()  # Load from settings
    container = ConsumerContainer(config)
    await container.init_resources()
    
    async for message in container.consumer.consume():
        await container.controller.handle(message)