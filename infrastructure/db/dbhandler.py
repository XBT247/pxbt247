from collections import defaultdict
import json
import aiomysql
import asyncio
from asyncio import Lock
from core.base_binance import KafkaBase

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
        self._active_connections = set()

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
                    maxsize=4, # Maximum connections in pool
                    pool_recycle=900,  # 15 minutes
                    connect_timeout=10,
                    autocommit=True,
                    cursorclass=aiomysql.DictCursor
                )
                self.logger.info("MySQL connection pool created successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create MySQL connection pool: {e}")
                raise

    async def execute_transaction(self, query, params=None, read_only=False, timeout=30):
        conn = None
        try:
            if not self.pool or self.pool._closed:
                await self.create_pool()
                
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                #if timeout:
                #    await cursor.execute(f"SET SESSION max_statement_time={timeout}")
                #if timeout and query.strip().upper().startswith('SELECT'):
                #    query = f"/*+ MAX_EXECUTION_TIME({timeout * 1000}) */ {query}"
                await cursor.execute(query, params or ())
                # For SELECT queries, fetch results
                
                if(query.strip().upper().startswith('SELECT') 
                    or query.strip().upper().startswith('SHOW')):
                    results = await cursor.fetchall()
                    """if results:
                        self.logger.info(f"QeuryExec fetchAll {query.strip().upper()} {len(results)} rows")
                    else:
                        self.logger.info(f"QeuryExec fetchAll {query.strip().upper()} no rows")
                    """
                    return results
                # For write operations, commit if not read_only
                elif not read_only:
                    await conn.commit()
                    return cursor.rowcount  # Return affected rows count
                
                # For read-only write operations (like SELECT FOR UPDATE)
                return None
        except (aiomysql.OperationalError, aiomysql.InterfaceError) as e:
            # Force reconnect on connection errors
            await self.close_pool()
            await self.create_pool()
            raise        
        except Exception as e:
            if conn:
                await conn.rollback()
            raise
        finally:
            if conn:
                await self._release_connection(conn)
    
    async def execute_transaction_batch(self, table_name, query, values):
        """Helper method for batch transactions"""
        conn = None
        try:
            if not self.pool or self.pool._closed:
                await self.create_pool()

            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.executemany(query, values)  # Execute batch
                await conn.commit()  # Commit the transaction
                return cursor.rowcount  # Return number of affected rows
        except Exception as e:
            self.logger.error(f"Batch insert failed for {table_name}: {str(e)}")
            if conn:
                await conn.rollback()
            raise
        finally:
            if conn:
                await self._release_connection(conn)

    async def save_trading_pair(self, trading_pair):
        """Save or update trading pair information in the database."""
        
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
        params = (
            trading_pair['symbol'].lower(),
            trading_pair['status'],
            trading_pair['base_asset'].lower(),
            trading_pair['quote_asset'].lower(),
            trading_pair['base_asset_precision'],
            trading_pair['quote_asset_precision'],
            trading_pair['is_spot_trading_allowed'],
            trading_pair['is_margin_trading_allowed'],
            json.dumps(trading_pair['filters']),
            trading_pair['maker_commission'],
            trading_pair['taker_commission'],
            trading_pair['max_margin_allowed']
        )
        try:
            cursor = await self.execute_transaction(query, params)
            self.logger.info(f"DB Saved/updated trading pair {trading_pair['symbol']}")
            return cursor
        except Exception as e:
            self.logger.error(f"DB Failed to save/update trading pair {trading_pair['symbol']}: {e}")
            #raise  # Re-raise if you want calling code to handle the exception        

    async def fetch_trading_pairs(self):
        """Fetch trading pairs from MySQL and return as a dictionary."""
        
        trading_pairs = {}
        query = "SELECT `symbol`, `status`, `base_asset`, `quote_asset`, `base_asset_precision`, `quote_asset_precision`, `is_spot_trading_allowed`, `is_margin_trading_allowed`, `filters`, `maker_commission`, `taker_commission`, `max_margin_allowed` FROM `tbl_trading_pairs`"  # Adjust table name

        try:
            results = await self.execute_transaction(query,read_only=True)
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
        query = "SHOW TABLES"
        try:            
            results = await self.execute_transaction(query,read_only=True)
            self.known_tables = {list(row.values())[0].lower() for row in results}
            self.logger.info(f"Loaded {len(self.known_tables)} known tables.")
        except Exception as e:
            self.logger.error(f"Failed to load known tables: {e}")
            raise

    async def ensure_table_exists(self, symbol):
        """Ensure table exists for the given symbol."""
        table_name = self._get_table_name(symbol)
        if table_name in self.known_tables:
            return True

        basetbl = "tbl_binance_base_btc" #if "btc" in symbol.lower() else "tbl_binance_base_usd"
        query = f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {basetbl}"        
        try:
            await self.execute_transaction(query)
            self.logger.info(f"DB Table created {table_name}")
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

        # Create all missing tables first
        missing_tables = [t for t in batches_to_process.keys() if t not in self.known_tables]
        for table in missing_tables:
            symbol = table.replace('tbl_binance_', '')
            await self.ensure_table_exists(symbol)

        logData = None
        # Prepare multi-table insert
        for table_name, records in batches_to_process.items():
            if not records:
                continue                        
            query = f"""
            INSERT INTO {table_name} 
            (serverTime, volume, totalVolumeSell, totalVolumeBuy, trades, totalTradesSell, 
                totalTradesBuy, open, close, high, low, qusd_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    item['et'], 
                    item['qusd'] if item.get('qusd', 0) > 0 else item['q'], 
                    item['qs'], 
                    item['qb'], 
                    item['t'], 
                    item['ts'], 
                    item['tb'], 
                    item['o'], 
                    item['c'], 
                    item['h'], 
                    item['l'], 
                    item.get('qp', 0) if item.get('qp') else 0
                ) 
                for item in records
            ]
            logData = f"{table_name}: {values}"
            try:
                affected_rows = await self.execute_transaction_batch(table_name, query, values)
                #self.logger.info(f"Inserted {affected_rows} rows into {table_name}")            
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
        
        total_inserted = sum(len(batch) for batch in batches_to_process.values())
        self.logger.info(f"Inserted {total_inserted} records across {len(batches_to_process)} tables")
        
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
        query = f"""
            INSERT INTO {table_name} 
            (serverTime, volume, totalVolumeSell, totalVolumeBuy, trades, totalTradesSell, 
                totalTradesBuy, open, close, high, low, qusd_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """        
        try:
           values = (data['et'], data['qusd'] if data.get('qusd', 0) > 0 else data['q'], data['qs'], data['qb']
                     , data['t'], data['ts'], data['tb'], data['o'], data['c'], data['h'], data['l']
                     , data.get('qp', 0) if data.get('qp') else 0)
           await self.execute_transaction(query,values)
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

    async def _get_connection(self):
        if not self.pool or self.pool._closed:
            self.logger.error("Connection pool is closed. Cannot acquire connection.")
            raise RuntimeError("Connection pool is closed.")
        conn = await self.pool.acquire()
        self.logger.debug(
            f"Acquired connection {id(conn)}. "
            f"Active: {len(self._active_connections)+1}/{self.pool.maxsize}"
        )
        self._active_connections.add(conn)
        return conn
        
    async def _release_connection(self, conn):
        if conn in self._active_connections:
            await self.pool.release(conn)
            self._active_connections.remove(conn)

    async def close_pool(self):
        """Cleanup resources"""
        if self.pool and not self.pool._closed:
            await self.stop_periodic_flusher()
            await self.flush_batches()
            for conn in list(self._active_connections):
                await self._release_connection(conn)
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("MySQL connection pool closed.")
        else:
            self.logger.warning("Attempted to close an already closed pool.")
