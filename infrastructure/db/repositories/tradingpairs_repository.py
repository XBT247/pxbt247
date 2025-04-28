# infra/db/mysql/repositories/trading_pairs.py
import json
from typing import Any, Dict, List
from core import logging
from infrastructure.db.BaseRepository import BaseRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from infrastructure.db.DBError import RepositoryError

class TradingPairsRepository(BaseRepository, ITradingPairsRepository):
    """MySQL implementation of trading pairs repository"""
    
    def __init__(self, db_config: Dict[str, Any], exchange: str, logger: logging.Logger = None):
        super().__init__(db_config)
        self.known_tables = set()
        self.exchange = exchange # e.g., 'binance'
        self.logger = logger

    async def save_trading_pair(self, pair_data: Dict[str, Any]) -> bool:
        query = """
            INSERT INTO tbl_trading_pairs (
                symbol, status, base_asset, quote_asset, 
                base_asset_precision, quote_asset_precision,
                is_spot_trading_allowed, is_margin_trading_allowed, 
                filters, maker_commission, taker_commission, max_margin_allowed
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
            pair_data['symbol'].lower(),
            pair_data['status'],
            pair_data['base_asset'].lower(),
            pair_data['quote_asset'].lower(),
            pair_data['base_asset_precision'],
            pair_data['quote_asset_precision'],
            pair_data['is_spot_trading_allowed'],
            pair_data['is_margin_trading_allowed'],
            json.dumps(pair_data['filters']),
            pair_data['maker_commission'],
            pair_data['taker_commission'],
            pair_data['max_margin_allowed']
        )
        try:
            await self._execute(query, params)
            return True
        except Exception as e:
            raise RepositoryError(f"Failed to save trading pair {pair_data['symbol']}", e)

    async def fetch_trading_pairs(self) -> Dict[str, Dict[str, str]]:
        query = """
            SELECT symbol, base_asset, quote_asset 
            FROM tbl_trading_pairs
        """
        try:
            results = await self._execute(query, read_only=True)
            return {
                row['symbol'].lower(): {
                    'base_asset': row['base_asset'].lower(),
                    'quote_asset': row['quote_asset'].lower()
                } for row in results
            }
        except Exception as e:
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