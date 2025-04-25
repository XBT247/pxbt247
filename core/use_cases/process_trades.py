# process_trades.py
import asyncio
import logging
from core.domain.trade import Trade, CachedTradeData
from core.interfaces.imessaging import IMessageProducer
from infrastructure.db.interfaces.icache_repository import ICacheRepository
from infrastructure.db.interfaces.itrade_repository import ITradesRepository

logger = logging.getLogger(__name__)

class ProcessTradeUseCase:
    def __init__(
        self,
        trade_repo: ITradesRepository,
        cache_repo: ICacheRepository,
        producer: IMessageProducer,
        max_cached_trades: int = 100  # Configurable
    ):
        self.trade_repo = trade_repo
        self.cache_repo = cache_repo
        self.producer = producer
        self.max_cached_trades = max_cached_trades

    async def execute(self, trade: Trade):
        try:
            # Get cached data with debug logging
            logger.debug(f"Processing trade for {trade.symbol}")
            cached_data = await self._get_cached_data(trade.symbol)
            
            updated_data = self._calculate_indicators(cached_data, trade)
            
            # Transactional save
            await self._persist_data(trade, updated_data)
            
            # Publish update
            await self._publish_update(trade.symbol, updated_data)
            
        except Exception as e:
            logger.error(f"Trade processing failed for {trade.symbol}: {e}", exc_info=True)
            raise

    async def _get_cached_data(self, symbol: str):
        """Helper method with logging"""
        try:
            data = await self.cache_repo.get(symbol)
            if not data:
                logger.info(f"No cached data found for {symbol}, initializing")
                data = CachedTradeData.initial(symbol)
            return data
        except Exception as e:
            logger.warning(f"Cache read failed for {symbol}: {e}")
            return CachedTradeData.initial(symbol)

    async def _persist_data(self, trade: Trade, updated_data: CachedTradeData):
        """Transactional persistence with logging"""
        try:
            await asyncio.gather(
                self.trade_repo.add(trade),
                self.cache_repo.update(trade.symbol, updated_data)
            )
            logger.debug(f"Persisted trade {trade.trade_id} for {trade.symbol}")
        except Exception as e:
            logger.error(f"Persistence failed for {trade.symbol}: {e}")
            raise

    async def _publish_update(self, symbol: str, data: CachedTradeData):
        """Publishing with logging"""
        try:
            await self.producer.send(
                topic="cache.binance.trendaware",
                key=symbol,
                value=data.to_dict()
            )
            logger.debug(f"Published update for {symbol}")
        except Exception as e:
            logger.error(f"Publish failed for {symbol}: {e}")
            raise

    def _calculate_indicators(self, cached_data: CachedTradeData, new_trade: Trade):
        """Indicator calculation with debug logging"""
        try:
            # ... existing calculation logic ...
            logger.debug(f"Calculated indicators for {new_trade.symbol}")
            return None #updated_data
        except Exception as e:
            logger.error(f"Indicator calculation failed: {e}")
            raise