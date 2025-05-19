# infra/db/mysql/repositories/trading_pairs.py
import json
from typing import Any, Dict, List, Optional
from core import logging
from core.domain.entities.trading_pair import TradingPair
from infrastructure.db.BaseRepository import BaseRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from infrastructure.db.DBError import RepositoryError
from infrastructure.db.models.trade_db_model import TradeDBModel

class TradingPairsRepository(BaseRepository, ITradingPairsRepository):
    """MySQL implementation of trading pairs repository"""
    
    def __init__(self, db_config: Dict[str, Any], exchange: str, logger: logging.Logger = None):
        super().__init__(db_config)
        self.known_tables = set()
        self.exchange = exchange # e.g., 'binance'
        self.logger = logger

    async def save_trading_pair(self, pair: TradingPair) -> bool:
        query = """
            INSERT INTO tbl_trading_pairs (
                symbol, status, base_asset, quote_asset, base_asset_precision, quote_asset_precision,
                is_spot_trading_allowed, is_margin_trading_allowed, filters, maker_commission, taker_commission, max_margin_allowed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                base_asset = VALUES(base_asset),
                quote_asset = VALUES(quote_asset),
                base_asset_precision = VALUES(base_asset_precision),
                quote_asset_precision = VALUES(quote_asset_precision),
                is_spot_trading_allowed = VALUES(is_spot_trading_allowed),
                is_margin_trading_allowed = VALUES(is_margin_trading_allowed),
                filters = VALUES(filters),
                maker_commission = VALUES(maker_commission),
                taker_commission = VALUES(taker_commission),
                max_margin_allowed = VALUES(max_margin_allowed)
        """
        params = (
            pair.symbol, pair.status, pair.base_asset, pair.quote_asset,
            pair.base_asset_precision, pair.quote_asset_precision,
            pair.is_spot_trading_allowed, pair.is_margin_trading_allowed,
            json.dumps(pair.filters), pair.maker_commission, 
            pair.taker_commission, pair.max_margin_allowed
        )        
        try:
            await self._execute(query, params)
            return True
        except Exception as e:
            raise RepositoryError(f"Failed to save trading pair {pair.symbol}", e)

    async def fetch_trading_pairs(self) -> List[TradingPair]:
        query = """
            SELECT 
                symbol, 
                status, 
                base_asset, 
                quote_asset, 
                base_asset_precision, 
                quote_asset_precision, 
                is_spot_trading_allowed, 
                is_margin_trading_allowed, 
                filters, 
                maker_commission, 
                taker_commission, 
                max_margin_allowed, 
                volume_slots, 
                trade_slots, 
                slots_stats
            FROM tbl_trading_pairs
        """
        try:
            #self.logger.info(f"Executing query: {query}")  # Added logging
            results = await self._execute(query, read_only=True)
            self.logger.info(f"Query results: {results[:5]}")  # Added logging
            trading_pairs = [
                TradingPair(
                    symbol=row['symbol'],
                    status=row['status'],
                    base_asset=row['base_asset'],
                    quote_asset=row['quote_asset'],
                    base_asset_precision=row['base_asset_precision'],
                    quote_asset_precision=row['quote_asset_precision'],
                    is_spot_trading_allowed=row['is_spot_trading_allowed'],
                    is_margin_trading_allowed=row['is_margin_trading_allowed'],
                    filters=json.loads(row['filters']) if row['filters'] else [],
                    maker_commission=row['maker_commission'],
                    taker_commission=row['taker_commission'],
                    max_margin_allowed=row['max_margin_allowed'],
                    volume_slots=row.get('volume_slots', '{}'),
                    trade_slots=row.get('trade_slots', '{}'),
                    slots_stats=row.get('slots_stats', '{}')
                )
                for row in results
            ]
            self.logger.info(f"Transformed results: {trading_pairs[0]}")  # Added logging
            return trading_pairs
        except Exception as e:
            self.logger.error(f"Error fetching trading pairs: {e}")  # Added logging
            self._log_error(f"Failed to execute query: {query}", e)  # Added logging
            raise RepositoryError("Failed to fetch trading pairs", e)

    async def ensure_table_exists(self, symbol: str) -> bool:
        table_name = self._get_table_name(symbol)
        if table_name in self.known_tables:
            return True
            
        query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE tbl_binance_base_btc"
        try:
            await self._execute(query)
            self.known_tables.add(table_name)
            return True
        except Exception as e:
            raise RepositoryError(f"Failed to create table for {symbol}", e)

    def _get_table_name(self, symbol: str) -> str:
        """Generate normalized table name"""
        return f"tbl_binance_{symbol.replace('.', '_').lower()}"
    
    async def update_slots_data(self, symbol: str, volume_slots: str, trade_slots: str, stats: str) -> bool:
        """Update slots data in trading pairs table"""
        query = """
            UPDATE tbl_trading_pairs
            SET volume_slots = %s,
                trade_slots = %s,
                slots_stats = %s,
                slots_updated_at = NOW()
            WHERE symbol = %s
        """
        await self._execute(query, (volume_slots, trade_slots, stats, symbol))

    async def fetch_slots_data(self, symbol: str) -> List[TradingPair]:
        """Fetch stored slots data for a symbol"""
        query = """
            SELECT volume_slots, trade_slots, slots_stats
            FROM tbl_trading_pairs
            WHERE symbol = %s
        """
        results = await self._execute(query, (symbol,))
        return results[0] if results else None


