from infrastructure.exchanges.binance.adapter import BinanceWebSocketAdapter
from core.use_cases.produce_trades import ProduceTradesUseCase
from application.services.trade_producer_service import TradeProducerService
from infrastructure.messaging.kafka_producer import KafkaProducerClient

class ProducerContainer:
    def __init__(self, config: dict):
        self.config = config
        self.exchange = config['exchange']
        
    async def initialize(self):
        # Initialize producer
        self.producer = KafkaProducerClient(
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            raw_topic=f"{self.exchange}.trades.raw",
            agg_topic=f"{self.exchange}.trades.agg"
        )
        await self.producer.initialize()
        
        # Initialize WebSocket adapter
        self.ws_adapter = BinanceWebSocketAdapter()
        
        # Initialize use case
        self.use_case = ProduceTradesUseCase(
            producer=self.producer,
            repository=None  # Would inject real repository
        )
        
        # Initialize service
        self.trade_service = TradeProducerService(
            use_case=self.use_case,
            ws_adapter=self.ws_adapter,
            symbols=self.config['symbols']
        )
    
    async def shutdown(self):
        await self.producer.stop()
        await self.ws_adapter.disconnect()