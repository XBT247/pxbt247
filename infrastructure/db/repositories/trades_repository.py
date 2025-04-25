# infra/db/mysql/repositories/trade_repository.py
import asyncio
import json
from typing import Dict, List, Any, Optional
from collections import OrderedDict, defaultdict
from infrastructure.db.BaseRepository import BaseRepository
from infrastructure.db.DBError import RepositoryError
from infrastructure.db.interfaces.itrade_repository import ITradesRepository

class TradesRepository(BaseRepository, ITradesRepository):
    """MySQL implementation of trade data repository"""
    
    def __init__(self, db_config: Dict[str, Any], exchange: str):
        super().__init__(db_config)
        self._batches = defaultdict(list)
        self._periodic_flusher_task = None
        self._batch_timeout = db_config.get('batch_timeout', 5.0)  # Default 5 seconds
        self.known_tables = set()
        self.exchange = exchange # e.g., 'binance'
        
    @property
    def batch_timeout(self) -> float:
        return self._batch_timeout
        
    async def load_known_tables(self) -> None:
        """Load known trading pair tables into memory"""
        try:
            # Query database for existing tables matching our pattern
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name LIKE 'tbl_binance_%'
            """
            results = await self._execute(query, (self.db_config['database'],), read_only=True)
            self.known_tables = {row['table_name'] for row in results}
            self.logger.info(f"Loaded {len(self.known_tables)} known tables")
        except Exception as e:
            raise RepositoryError("Failed to load known tables", e)
            
    async def add_to_batch(self, symbol: str, trade_data: Dict[str, Any]) -> None:
        """
        Add trade data to batch for later insertion
        Args:
            symbol: Trading symbol (e.g., 'btcusdt')
            trade_data: Dictionary containing trade data
        """
        try:
            # Validate trade data
            required_fields = ['price', 'quantity', 'timestamp', 'is_buyer_maker']
            if not all(field in trade_data for field in required_fields):
                raise ValueError("Trade data missing required fields")
                
            table_name = self._get_table_name(symbol)
            if table_name not in self.known_tables:
                await self._ensure_table_exists(symbol)
                self.known_tables.add(table_name)
                
            self._batches[table_name].append(trade_data)
        except Exception as e:
            raise RepositoryError(f"Failed to add trade to batch for {symbol}", e)
            
    async def flush_batches(self) -> None:
        """Flush all batched trade data to database"""
        if not self._batches:
            return
            
        try:
            for table_name, trades in self._batches.items():
                if trades:
                    query = f"""
                        INSERT INTO {table_name} (
                            timestamp, price, quantity, is_buyer_maker,
                            trade_id, quote_quantity
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    params = [
                        (
                            trade['timestamp'],
                            trade['price'],
                            trade['quantity'],
                            trade['is_buyer_maker'],
                            trade.get('trade_id'),
                            trade.get('quote_quantity')
                        ) for trade in trades
                    ]
                    await self._execute_many(query, params)
                    self.logger.info(f"Inserted {len(trades)} trades into {table_name}")
                    
            self._batches.clear()
        except Exception as e:
            raise RepositoryError("Failed to flush trade batches", e)
            
    async def start_periodic_flusher(self) -> None:
        """Start background task for periodic batch flushing"""
        if self._periodic_flusher_task is None or self._periodic_flusher_task.done():
            self._periodic_flusher_task = asyncio.create_task(self._periodic_flush_loop())
            
    async def _periodic_flush_loop(self) -> None:
        """Background task that periodically flushes batches"""
        while True:
            await asyncio.sleep(self.batch_timeout)
            try:
                await self.flush_batches()
            except Exception as e:
                self.logger.error(f"Periodic flush failed: {e}")
                
    async def _ensure_table_exists(self, symbol: str) -> None:
        """Ensure table exists for given symbol"""
        table_name = self._get_table_name(symbol)
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                timestamp BIGINT NOT NULL,
                price DECIMAL(18, 8) NOT NULL,
                quantity DECIMAL(18, 8) NOT NULL,
                is_buyer_maker BOOLEAN NOT NULL,
                trade_id BIGINT,
                quote_quantity DECIMAL(18, 8),
                INDEX idx_timestamp (timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        await self._execute(query)
        
    def _get_table_name(self, symbol: str) -> str:
        """Generate normalized table name"""
        return f"tbl_binance_{symbol.replace('.', '_').lower()}"
