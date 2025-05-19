# infra/db/mysql/repositories/trade_repository.py
import asyncio
import json
import time
from typing import Dict, List, Any, Optional
from collections import OrderedDict, defaultdict
from core import logging
from core.domain.trade import RawTrade
from infrastructure.db.BaseRepository import BaseRepository
from infrastructure.db.DBError import RepositoryError
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.models.trade_db_model import TradeDBModel

class TradesRepository(BaseRepository, ITradesRepository):
    """MySQL implementation of trade data repository"""
    
    def __init__(self, db_config: Dict[str, Any], exchange: str, logger: logging.Logger = None):
        super().__init__(db_config)
        self._batches = defaultdict(list)
        self._periodic_flusher_task = None
        self._batch_timeout = db_config.get('batch_timeout', 5.0)  # Default 5 seconds
        self.known_tables = set()
        self.exchange = exchange # e.g., 'binance'
        self.logger = logger
        
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

    async def add(self, trade: RawTrade) -> None:
        """Add a single trade to the repository."""
        table_name = self._get_table_name(trade.symbol)
        # if table_name not in self.known_tables:
        #     await self._ensure_table_exists(trade.symbol)
        #     self.known_tables.add(table_name)

        # query = f"""
        #     INSERT INTO {table_name} (
        #         timestamp, price, quantity, is_buyer_maker,
        #         trade_id, quote_quantity
        #     ) VALUES (%s, %s, %s, %s, %s, %s)
        # """
        # params = (
        #     trade.timestamp,
        #     trade.price,
        #     trade.quantity,
        #     trade.is_buyer_maker,
        #     trade.trade_id,
        #     trade.quote_quantity
        # )
        try:
            # await self._execute(query, params)
            pass  # Disabled MySQL insertion
        except Exception as e:
            self.logger.error(f"Failed to add trade to {table_name}: {e}")
            raise

    async def fetch_trades_history(self, symbol: str, lookback_days: int = 0, total_records: int = 0, order_asc: bool = True) -> List[TradeDBModel]:
        """Fetch historical volume and trades data for a symbol and return as TradingPair objects"""
        table_name = self._get_table_name(symbol)
        conditions = []
        params = []

        # Add serverTime filter if lookback_days > 0
        if lookback_days > 0:
            millis_now = int(time.time() * 1000)
            millis_delta = lookback_days * 24 * 60 * 60 * 1000
            from_timestamp = millis_now - millis_delta
            conditions.append("serverTime >= %s")
            params.append(from_timestamp)

        # Build base query
        query = f"""
            SELECT id, serverTime, open, close, avgPrice, high, low, high200, low200,
                qusd_price, volume, trades, totalVolumeBuy, totalVolumeSell, 
                actualVolSell, actualVolBuy, actualVolume, totalTradesBuy, 
                totalTradesSell, buyPercentage, volumePoints, tradesPoints, 
                sumVolumePoints100, sumTradePoints100, qtr_history30, qtr_history100, 
                qtr_avgHistory100, order_side, order_type, order_price, 
                order_text, isSidelined, dir
            FROM {table_name}
        """

        # Add WHERE clause if needed
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        # Add ORDER BY clause
        #query += " ORDER BY serverTime " + ("ASC" if order_asc else "DESC")

        # Add LIMIT if requested
        if total_records > 0:
            query += " LIMIT %s"
            params.append(total_records)

        try:
            rows = await self._execute(query, tuple(params))
            self.logger.info(f"fetch_trades_history Fetched {len(rows)} rows from {table_name}")
            trade_models = [
                TradeDBModel(
                    id=row['id'],
                    server_time=row['serverTime'],
                    open=row['open'],
                    close=row['close'],
                    avg_price=row['avgPrice'],
                    high=row['high'],
                    low=row['low'],
                    high200=row['high200'],
                    low200=row['low200'],
                    qusd_price=row['qusd_price'],
                    volume=row['volume'],
                    trades=row['trades'],
                    total_volume_buy=row['totalVolumeBuy'],
                    total_volume_sell=row['totalVolumeSell'],
                    actual_vol_sell=row['actualVolSell'],
                    actual_vol_buy=row['actualVolBuy'],
                    actual_volume=row['actualVolume'],
                    total_trades_buy=row['totalTradesBuy'],
                    total_trades_sell=row['totalTradesSell'],
                    buy_percentage=row['buyPercentage'],
                    volume_points=row['volumePoints'],
                    trades_points=row['tradesPoints'],
                    sum_volume_points_100=row['sumVolumePoints100'],
                    sum_trade_points_100=row['sumTradePoints100'],
                    qtr_history_30=row['qtr_history30'],
                    qtr_history_100=row['qtr_history100'],
                    qtr_avg_history_100=row['qtr_avgHistory100'],
                    order_side=row['order_side'],
                    order_type=row['order_type'],
                    order_price=row['order_price'],
                    order_text=row['order_text'],
                    is_sidelined=row['isSidelined'],
                    dir=row['dir']
                )
                for row in rows
            ]

            #if trade_models:
            #    self.logger.info(f"Sample TradeDBModel: {trade_models[0]}")

            return trade_models
            
        except Exception as e:
            self._log_error(f"Failed to fetch volume/trades history for {symbol}", e)
            raise RepositoryError(f"Failed to fetch volume/trades history for {symbol}", e)