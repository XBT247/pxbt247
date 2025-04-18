# trade_repository.py
import logging
from typing import List
from core.domain.trade import Trade
from infrastructure.db.BaseRepository import BaseRepository
from infrastructure.db.interfaces.itrade_repository import ITradesRepository


logger = logging.getLogger(__name__)

class TradeRepository(BaseRepository, ITradesRepository):
    def __init__(self, db_config: dict):
        super().__init__(db_config)
        self._batches = {}
        self.known_tables = set()

    async def load_known_tables(self):
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_name LIKE 'tbl_binance_%'
        """
        results = await self._execute(query, (self.db_config['database'],), read_only=True)
        self.known_tables = {row['table_name'] for row in results}

    async def add(self, trade: Trade):
        table_name = self._get_table_name(trade.symbol)
        if table_name not in self.known_tables:
            await self._create_table(table_name)
            self.known_tables.add(table_name)

        if table_name not in self._batches:
            self._batches[table_name] = []
            
        self._batches[table_name].append(trade)
        
        if len(self._batches[table_name]) >= 100:  # Flush if batch size reaches 100
            await self._flush_table(table_name)

    async def add_batch(self, trades: List[Trade]):
        for trade in trades:
            await self.add(trade)

    async def _flush_table(self, table_name: str):
        if table_name not in self._batches or not self._batches[table_name]:
            return
            
        query = f"""
            INSERT INTO {table_name} (
                timestamp, price, quantity, is_buyer_maker,
                trade_id, quote_quantity
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = [
            (
                trade.timestamp,
                trade.price,
                trade.quantity,
                trade.is_buyer_maker,
                trade.trade_id,
                trade.quote_quantity
            ) for trade in self._batches[table_name]
        ]
        
        try:
            await self._execute_many(query, params)
            logger.info(f"Inserted {len(params)} trades into {table_name}")
            self._batches[table_name].clear()
        except Exception as e:
            logger.error(f"Failed to flush trades to {table_name}: {e}")
            raise

    async def _create_table(self, table_name: str):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                timestamp DATETIME NOT NULL,
                price DECIMAL(18, 8) NOT NULL,
                quantity DECIMAL(18, 8) NOT NULL,
                is_buyer_maker BOOLEAN NOT NULL,
                trade_id VARCHAR(50),
                quote_quantity DECIMAL(18, 8),
                INDEX idx_timestamp (timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        await self._execute(query)

    def _get_table_name(self, symbol: str) -> str:
        return f"tbl_binance_{symbol.replace('.', '_').lower()}"
