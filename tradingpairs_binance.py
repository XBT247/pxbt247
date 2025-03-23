import asyncio
import json
import time
import aiohttp
import hmac
import hashlib
import urllib.parse
from base_binance import KafkaBase

class TradingPairsFetcher(KafkaBase):
    def __init__(self):
        super().__init__()

    async def fetch_trading_pairs(self):
        """Fetch trading pairs from Binance and store crucial fields in the database."""
        self.logger.info("Starting to fetch trading pairs...")
        try:
            self.logger.info("Creating aiohttp ClientSession...")
            timeout = aiohttp.ClientTimeout(total=30)  # Set a 30-second timeout
            async with aiohttp.ClientSession(timeout=timeout) as session:
                self.logger.info("ClientSession created successfully.")

                # Fetch account information to get commission rates
                self.logger.info("Fetching account information...")                
                headers = {"X-MBX-APIKEY": self.config_binance["api_key"]}  # Add your API key here
                async with session.get(self.config_binance_urls["account"], headers=headers) as response:
                    account_data = await response.json()
                    maker_commission = account_data.get('makerCommission', 10) / 10000  # Convert to decimal (e.g., 10 -> 0.001)
                    taker_commission = account_data.get('takerCommission', 15) / 10000  # Convert to decimal (e.g., 15 -> 0.0015)
                    self.logger.info(f"Maker commission: {maker_commission}, Taker commission: {taker_commission}")

                # Fetch Spot trading pairs
                self.logger.info("Fetching Spot trading pairs...")                
                async with session.get(self.config_binance_urls["exchange_info_spot"]) as response:
                    self.logger.info("Received response from Spot API.")
                    spot_data = await response.json()  # Load JSON response

                    # Extract distinct quote currencies
                    self.quote_currencies = {}  # Reset quote currencies

                    # Store trading pair information in the database and Kafka cache
                    for symbol_info in spot_data['symbols']:
                        if symbol_info['status'] == 'TRADING':  # Only include active pairs
                            # Extract crucial fields
                            trading_pair = {
                                'symbol': symbol_info['symbol'].lower(),
                                'status': symbol_info['status'],
                                'base_asset': symbol_info['baseAsset'],
                                'quote_asset': symbol_info['quoteAsset'],
                                'base_asset_precision': symbol_info['baseAssetPrecision'],
                                'quote_asset_precision': symbol_info['quoteAssetPrecision'],
                                'is_spot_trading_allowed': symbol_info['isSpotTradingAllowed'],
                                'is_margin_trading_allowed': symbol_info['isMarginTradingAllowed'],
                                'filters': symbol_info['filters'],  # Store all filters
                                'maker_commission': maker_commission,  # Add maker commission
                                'taker_commission': taker_commission,  # Add taker commission
                                'max_margin_allowed': 1000.0 if symbol_info['isMarginTradingAllowed'] else 0,  # Example value
                                'is_marginable': False,  # Default value
                                'cross_margin_ratio': 0.0,  # Default value
                                'is_isolated_margin': False,  # Default value
                                'isolated_borrow_limit_base': 0.0,  # Default value
                                'isolated_borrow_limit_quote': 0.0  # Default value
                            }

                            # Fetch margin information for the trading pair
                            """if symbol_info['isMarginTradingAllowed']:
                                margin_info = await self.fetch_margin_info(session, symbol_info['symbol'])
                                if margin_info:
                                    trading_pair.update({
                                        'is_marginable': margin_info.get('marginable', False),
                                        'cross_margin_ratio': float(margin_info.get('crossMarginRatio', 0.0)),
                                        'is_isolated_margin': margin_info.get('isolated', False),
                                        'isolated_borrow_limit_base': float(margin_info.get('baseBorrowLimit', 0.0)),
                                        'isolated_borrow_limit_quote': float(margin_info.get('quoteBorrowLimit', 0.0))
                                    })"""

                            # Add quote currency to the set
                            self.quote_currencies[symbol_info['quoteAsset'].lower()] = 0.0  # Initialize price to 0.0

                            # Publish trading pair to Kafka cache topic
                            await self.publish_to_kafka_cache(trading_pair)

                            # Save trading pair information to the database
                            #await self.save_trading_pair_to_db(trading_pair)

                    self.logger.info(f"Distinct quote currencies: {self.quote_currencies}")
                
                # Fetch Futures trading pairs
                self.logger.info("Fetching Futures trading pairs...")
                async with session.get(self.config_binance_urls["exchange_info_future"]) as response:
                    self.logger.info("Received response from Futures API.")
                    futures_data = await response.json()
                    futures_pairs = [symbol['symbol'] for symbol in futures_data['symbols']]
                    self.logger.info(f"Fetched {len(futures_pairs)} Futures trading pairs.")

                # Fetch Perpetual trading pairs
                self.logger.info("Fetching Perpetual trading pairs...")
                async with session.get(self.config_binance_urls["exchange_info_perp"]) as response:
                    self.logger.info("Received response from Perpetual API.")
                    perpetual_data = await response.json()
                    perpetual_pairs = [symbol['symbol'] for symbol in perpetual_data['symbols']]
                    self.logger.info(f"Fetched {len(perpetual_pairs)} Perpetual trading pairs.")
                
                # Exclude high-load pairs from the trading pairs list
                pairs = list(set([symbol_info['symbol'].lower() for symbol_info in spot_data['symbols'] if symbol_info['status'] == 'TRADING'])
                             #+ set([symbol_info['symbol'].lower() for symbol_info in spot_data['symbols'] if symbol_info['status'] == 'TRADING']) 
                             #+ set([symbol_info['symbol'].lower() for symbol_info in spot_data['symbols'] if symbol_info['status'] == 'TRADING']) 
                             )
                self.trading_pairs = list(set(pairs) - set(self.highload_pairs))
                self.logger.info(f"Total trading pairs fetched (excluding high-load pairs): {len(self.trading_pairs)}")
        except asyncio.TimeoutError:
            self.logger.error("Timeout while fetching trading pairs.")
        except aiohttp.ClientError as e:
            self.logger.error(f"Network error fetching trading pairs: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error fetching trading pairs: {e}")
            raise

    async def fetch_margin_info(self, session, symbol):
        """Fetch margin information for a trading pair from Binance's API."""
        try:
            # Add timestamp and generate signature
            params = {
                "symbol": symbol,
                "timestamp": int(time.time() * 1000)  # Current time in milliseconds
            }
            params["signature"] = self.generate_signature(params)
            headers = {"X-MBX-APIKEY": self.config_binance["api_key"]}  # Add your API key here
            async with session.get(self.config_binance_urls["margin_isolated_pair"], headers=headers, params=params) as response:
                if response.status == 200:
                    margin_data = await response.json()
                    self.logger.info(f"Fetched margin info for {symbol}: {margin_data}")

                    # Check if the response contains valid data
                    if isinstance(margin_data, list) and len(margin_data) > 0:
                        margin_info = margin_data[0]  # Use the first result
                        return {
                            'marginable': margin_info.get('marginable', False),
                            'cross_margin_ratio': float(margin_info.get('crossMarginRatio', 0.0)),
                            'isolated': margin_info.get('isolated', False),
                            'baseBorrowLimit': float(margin_info.get('baseBorrowLimit', 0.0)),
                            'quoteBorrowLimit': float(margin_info.get('quoteBorrowLimit', 0.0))
                        }
                    else:
                        self.logger.warning(f"No margin data found for {symbol}.")
                        return None
                else:
                    self.logger.error(f"Failed to fetch margin info for {symbol}. Status: {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching margin info for {symbol}: {e}")
            return None
    def generate_signature(self, params):
        """Generate a HMAC-SHA256 signature for the request."""
        query_string = urllib.parse.urlencode(params)
        return hmac.new(self.config_binance["secret_key"].encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def save_trading_pair_to_db(self, trading_pair):
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
        async with await self.create_db_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, (
                        trading_pair['symbol'],
                        trading_pair['status'],
                        trading_pair['base_asset'],
                        trading_pair['quote_asset'],
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
                    self.logger.info(f"Saved/updated trading pair {trading_pair['symbol']} in database.")
                except Exception as e:
                    self.logger.error(f"Failed to save/update trading pair {trading_pair['symbol']} in database: {e}")
                    await conn.rollback()

    async def publish_to_kafka_cache(self, trading_pair):
        """Publish trading pair to Kafka cache topic."""
        try:
             # Maintain local cache
            self.trading_pairs[trading_pair['symbol']] = trading_pair
            await self.producer.send(self.topicTradingPairsCache, key=trading_pair['symbol'].encode('utf-8'), value=trading_pair)
            self.logger.info(f"Published trading pair {trading_pair['symbol']} to Kafka cache topic.")
        except Exception as e:
            self.logger.error(f"Failed to publish trading pair {trading_pair['symbol']} to Kafka cache topic: {e}")

    async def run(self):
        """Main method to run the trading pairs fetcher."""
        
        # Wait for Kafka to be ready
        if not await self.wait_for_kafka():
            self.logger.error("Exiting due to Kafka initialization failure.")
            return
        
        # Create Kafka producer
        self.producer = await self.create_kafka_producer()

        # Run fetch_trading_pairs every hour
        while True:
            await self.fetch_trading_pairs()
            await asyncio.sleep(3600)  # Wait for 1 hour