from core.domain.trade import Trade, CachedTradeData
from core.interfaces.imessaging import IMessageProducer
from infrastructure.db.interfaces.icache_repository import ICacheRepository
from infrastructure.db.interfaces.itrade_repository import ITradesRepository

class ProcessTradeUseCase:
    def __init__(
        self,
        trade_repo: ITradesRepository,
        cache_repo: ICacheRepository,
        producer: IMessageProducer
    ):
        self.trade_repo = trade_repo
        self.cache_repo = cache_repo
        self.producer = producer

    async def execute(self, trade: Trade):
        # Get or initialize cached data
        cached_data = await self.cache_repo.get(trade.symbol)
        if not cached_data:
            cached_data = CachedTradeData(
                symbol=trade.symbol,
                trades=[],
                rsi=0,
                macd=0,
                ma20=0
            )

        # Update indicators
        updated_data = self._calculate_indicators(cached_data, trade)
        
        # Persist data
        await self.trade_repo.add(trade)
        await self.cache_repo.update(trade.symbol, updated_data)
        
        # Publish to Kafka
        await self.producer.send(
            topic="cache.binance.trendaware",
            key=trade.symbol,
            value=updated_data.to_dict()
        )

    def _calculate_indicators(self, cached_data: CachedTradeData, new_trade: Trade):
        # Update trades list
        updated_trades = cached_data.trades + [new_trade]
        
        # Simple indicator calculations (replace with real logic)
        prices = [t.price for t in updated_trades[-20:]]  # Last 20 trades
        avg_price = sum(prices) / len(prices) if prices else new_trade.price
        
        return CachedTradeData(
            symbol=cached_data.symbol,
            trades=updated_trades[-100:],  # Keep last 100 trades
            rsi=avg_price * 1.1,
            macd=avg_price * 0.9,
            ma20=avg_price
        )