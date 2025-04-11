from collections import defaultdict
import json
import aiomysql
import asyncio
from asyncio import Lock
from base_binance import KafkaBase

class DBHandler(KafkaBase):
    def __init__(self, db_config):
        super().__init__()  # Initialize KafkaBase (inherits logger)
        self.db_config = db_config
        self.pool = None
        self.known_tables = set()
        self.insert_queue = []
        self.batch_size = 200  # Number of records to batch before insert
        self.batch_timeout = 3  # Max seconds to wait before flushing batch
        self.current_batches = defaultdict(list)  # {table_name: [records]}
        self.batch_lock = Lock()
        self.flush_lock = Lock()  # Separate lock for flushing
        self.error_count = 0
        self.MAX_ERRORS = 5
        self._flush_task = None

    def _get_table_name(self, symbol):
        """Generate normalized table name from symbol."""
        clean_symbol = symbol.replace(".", "_").lower()
        return f"tbl_binance_{clean_symbol}"
    
    async def create_pool(self):
        """Create and initialize MySQL connection pool."""
        if not self.pool:
            try:
                self.pool = await aiomysql.create_pool(
                    host=self.db_config['host'],
                    port=self.db_config['port'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    db=self.db_config['database'],
                    minsize=1,  # Minimum connections in pool
                    maxsize=10, # Maximum connections in pool
                    autocommit=True
                )
                self.logger.info("MySQL connection pool created successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create MySQL connection pool: {e}")
                raise
    
    async def save_trading_pair(self, trading_pair):
        """Save or update trading pair information in the database."""
        if not self.pool:
            await self.create_pool()
        query = """
            INSERT INTO tbl_trading_pairs (
                symbol, status, base_asset, quote_asset, base_asset_precision, quote_asset_precision,
                is_spot_trading_allowed, is_margin_trading_allowed, filters, maker_commission, taker_commission, max_margin_allowed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, (
                        trading_pair['symbol'].lower(),
                        trading_pair['status'],
                        trading_pair['base_asset'].lower(),
                        trading_pair['quote_asset'].lower(),
                        trading_pair['base_asset_precision'],
                        trading_pair['quote_asset_precision'],
                        trading_pair['is_spot_trading_allowed'],
                        trading_pair['is_margin_trading_allowed'],
                        json.dumps(trading_pair['filters']),  # Store filters as JSON
                        trading_pair['maker_commission'],
                        trading_pair['taker_commission'],
                        trading_pair['max_margin_allowed']
                    ))
                    await conn.commit()
                    self.logger.info(f"DB Saved/updated trading pair {trading_pair['symbol']}")
        except Exception as e:
            self.logger.error(f"DB Failed to save/update trading pair {trading_pair['symbol']}: {e}")

    async def fetch_trading_pairs(self):
        """Fetch trading pairs from MySQL and return as a dictionary."""
        if not self.pool:
            await self.create_pool()
        
        trading_pairs = {}
        query = "SELECT `symbol`, `status`, `base_asset`, `quote_asset`, `base_asset_precision`, `quote_asset_precision`, `is_spot_trading_allowed`, `is_margin_trading_allowed`, `filters`, `maker_commission`, `taker_commission`, `max_margin_allowed` FROM `tbl_trading_pairs`"  # Adjust table name

        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query)
                    results = await cursor.fetchall()
                    for row in results:
                        #if row['status'].lower() != 'trading':
                        trading_pairs[row['symbol'].lower()] = {
                            'base_asset': row['base_asset'].lower(),
                            'quote_asset': row['quote_asset'].lower()
                        }

            self.logger.info(f"Fetched {len(trading_pairs)} trading pairs from MySQL.")
        except Exception as e:
            self.logger.error(f"Error fetching trading pairs: {e}")

        return trading_pairs

    async def load_known_tables(self):
        """Load all known table names from the database."""
        if not self.pool:
            await self.create_pool()

        query = "SHOW TABLES"
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    results = await cursor.fetchall()
                    self.known_tables = {row[0].lower() for row in results}
            self.logger.info(f"Loaded {len(self.known_tables)} known tables.")
        except Exception as e:
            self.logger.error(f"Failed to load known tables: {e}")

    async def ensure_table_exists(self, symbol):
        """Ensure table exists for the given symbol."""
        table_name = self._get_table_name(symbol)
        if table_name in self.known_tables:
            return True

        basetbl = "tbl_binance_base_btc" #if "btc" in symbol.lower() else "tbl_binance_base_usd"
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} LIKE {basetbl}
                    """)
                    await conn.commit()
                    self.known_tables.add(table_name)
                    return True
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            return False
        
    async def add_to_batch(self, symbol, data):
        """Add trade data to batch with optimized locking"""
        table_name = self._get_table_name(symbol)
        
        async with self.batch_lock:
            self.current_batches[table_name].append(data)
            total = sum(len(b) for b in self.current_batches.values())
            
            if total >= self.batch_size and not self.flush_lock.locked():
                # Schedule flush without waiting
                asyncio.create_task(self._safe_flush())
                await asyncio.sleep(0.01)  # Small sleep between chunks

    async def _safe_flush(self):
        """Safe flushing with lock management"""
        async with self.flush_lock:
            await self._flush_batches()

    async def _flush_batches(self):
        """Flush all pending batches to database."""
         # Quickly get batches to process
        async with self.batch_lock:
            if not self.current_batches:
                return
                
            batches_to_process = {
                k: v.copy() for k, v in self.current_batches.items() if v
            }
            self.current_batches.clear()

        if not batches_to_process:
            return

        if not self.pool:
            await self.create_pool()

        # Create all missing tables first
        missing_tables = [t for t in batches_to_process.keys() if t not in self.known_tables]
        for table in missing_tables:
            symbol = table.replace('tbl_binance_', '')
            await self.ensure_table_exists(symbol)

        logData = None
        # Prepare multi-table insert
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    for table_name, records in batches_to_process.items():
                        if not records:
                            continue                        
                        query = f"""
                            INSERT INTO {table_name} 
                            (serverTime, volume, totalVolumeSell, totalVolumeBuy, trades, totalTradesSell, 
                             totalTradesBuy, open, close, high, low)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        values = [
                            (
                                item['et'], item['qusd'] if item.get('qusd', 0) > 0 else item['q'], item['qs'], item['qb'], item['t'], item['ts'], item['tb'],
                                item['o'], item['c'], item['h'], item['l']
                            ) for item in records
                        ]
                        logData = f"{table_name}: {values}"
                        await cursor.executemany(query, values)
                    
                    await conn.commit()
                    total_inserted = sum(len(batch) for batch in batches_to_process.values())
                    self.logger.info(f"Inserted {total_inserted} records across {len(batches_to_process)} tables")
                    
        except Exception as e:
            self.logger.error(f"Batch insert failed: {e} {logData}")
            self.error_count += 1
            if self.error_count > self.MAX_ERRORS:
                raise RuntimeError("Too many consecutive DB errors")
                
            # Re-add failed batches
            async with self.batch_lock:
                for table, records in batches_to_process.items():
                    self.current_batches[table].extend(records)
        finally:
            self.current_batches.clear()

    async def flush_batches(self):
        """Public flush method that coordinates with other operations"""
        if self.flush_lock.locked():
            return  # Don't overlap with active flush
            
        await self._safe_flush()

    async def _fallback_single_inserts(self):
        """Fallback to individual inserts when batch fails."""
        success_count = 0
        for table_name, records in self.current_batches.items():
            for record in records:
                try:
                    await self.single_insert(table_name, record)
                    success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to insert record into {table_name}: {e}")
                    raise
        
        self.logger.info(f"Fallback succeeded for {success_count} records")

    async def single_insert(self, table_name, data):
        """Insert single record into database."""
        if not self.pool:
            await self.create_pool()

        query = f"""
            INSERT INTO {table_name} 
            (serverTime, volume, totalVolumeSell, totalVolumeBuy, trades, totalTradesSell, 
                totalTradesBuy, open, close, high, low)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, (
                        data['et'], data['qusd'] if data.get('qusd', 0) > 0 else data['q'], data['qs'], data['qb'], data['t'], data['ts'], data['tb'],
                                data['o'], data['c'], data['h'], data['l']
                    ))
                    await conn.commit()
        except Exception as e:
            self.logger.error(f"Single insert failed for {table_name}: {e}")
            raise

    async def start_periodic_flusher(self):
        """Start background periodic flusher"""
        async def _flusher():
            while True:
                await asyncio.sleep(self.batch_timeout)
                try:
                    await self.flush_batches()
                except Exception as e:
                    self.logger.error(f"Periodic flush failed: {e}")

        self._flush_task = asyncio.create_task(_flusher())

    async def stop_periodic_flusher(self):
        """Stop background periodic flusher"""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

    async def close_pool(self):
        """Cleanup resources"""
        await self.stop_periodic_flusher()
        await self.flush_batches()
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("MySQL connection pool closed.")
