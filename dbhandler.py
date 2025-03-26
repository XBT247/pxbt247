import json
import aiomysql
import asyncio
from base_binance import KafkaBase

class DBHandler(KafkaBase):
    def __init__(self, db_config):
        super().__init__()  # Initialize KafkaBase (inherits logger)
        self.db_config = db_config
        self.pool = None

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

    async def close_pool(self):
        """Close the database connection pool."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.logger.info("MySQL connection pool closed.")
