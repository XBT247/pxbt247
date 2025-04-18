import json
from aiokafka import AIOKafkaProducer
from core.domain.trade import RawTrade, AggregatedTrade
from core.interfaces.iproducer import ITradeProducer

class KafkaProducerClient(ITradeProducer):
    def __init__(self, bootstrap_servers: str, raw_topic: str, agg_topic: str):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.raw_topic = raw_topic
        self.agg_topic = agg_topic

    async def initialize(self):
        await self.producer.start()

    async def produce(self, trade: RawTrade):
        await self.producer.send(
            topic=self.raw_topic,
            key=trade.symbol.encode(),
            value={
                'price': trade.price,
                'quantity': trade.quantity,
                'timestamp': trade.timestamp.isoformat(),
                'is_buyer_maker': trade.is_buyer_maker,
                'trade_id': trade.trade_id
            }
        )

    async def produce_aggregated(self, trade: AggregatedTrade):
        await self.producer.send(
            topic=self.agg_topic,
            key=trade.symbol.encode(),
            value={
                'open': trade.open_price,
                'close': trade.close_price,
                'high': trade.high_price,
                'low': trade.low_price,
                'total_quantity': trade.total_quantity,
                'total_trades': trade.total_trades,
                'buy_quantity': trade.buy_quantity,
                'sell_quantity': trade.sell_quantity,
                'start_time': trade.start_time.isoformat(),
                'end_time': trade.end_time.isoformat()
            }
        )

    async def stop(self):
        await self.producer.stop()