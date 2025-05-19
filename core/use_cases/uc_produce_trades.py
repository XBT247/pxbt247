
from core.domain.trade import RawTrade, AggregatedTrade
from core.interfaces.iproducer import ITradeProducer
from infrastructure.db.interfaces.itrade_repository import ITradesRepository

class ProduceTradesUseCase:
    def __init__(
        self,
        producer: ITradeProducer,
        repository: ITradesRepository,
        aggregation_window: int = 5  # seconds
    ):
        self.producer = producer
        self.repository = repository
        self.aggregation_window = aggregation_window
        self._aggregation_buffer = {}

    async def execute(self, trade: RawTrade):
        # Publish raw trade immediately
        await self.producer.produce(trade)
        
        # Aggregate trades
        await self._aggregate_trade(trade)
        
        # Store in repository if needed
        await self.repository.add(trade)

    async def _aggregate_trade(self, trade: RawTrade):
        symbol = trade.symbol
        if symbol not in self._aggregation_buffer:
            self._aggregation_buffer[symbol] = {
                'open': trade.price,
                'high': trade.price,
                'low': trade.price,
                'total_qty': 0,
                'buy_qty': 0,
                'sell_qty': 0,
                'trades': 0,
                'start_time': trade.timestamp
            }
        
        buffer = self._aggregation_buffer[symbol]
        buffer['high'] = max(buffer['high'], trade.price)
        buffer['low'] = min(buffer['low'], trade.price)
        buffer['total_qty'] += trade.quantity
        buffer['trades'] += 1
        
        if trade.is_buyer_maker:
            buffer['sell_qty'] += trade.quantity
        else:
            buffer['buy_qty'] += trade.quantity
            
        # Check if aggregation window has passed
        if (trade.timestamp - buffer['start_time']).total_seconds() >= self.aggregation_window:
            aggregated = AggregatedTrade(
                symbol=symbol,
                open_price=buffer['open'],
                close_price=trade.price,
                high_price=buffer['high'],
                low_price=buffer['low'],
                total_quantity=buffer['total_qty'],
                total_trades=buffer['trades'],
                buy_quantity=buffer['buy_qty'],
                sell_quantity=buffer['sell_qty'],
                start_time=buffer['start_time'],
                end_time=trade.timestamp
            )
            
            await self.producer.produce_aggregated(aggregated)
            del self._aggregation_buffer[symbol]