import asyncio
import aiohttp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def fetch_trading_pairs():
    """Fetch trading pairs from Binance (Spot, Futures, Perpetual)."""
    logger.info("Starting to fetch trading pairs...")
    try:
        # Set a timeout for the entire operation
        timeout = aiohttp.ClientTimeout(total=30)  # 30 seconds
        async with aiohttp.ClientSession(timeout=timeout) as session:
            logger.info("ClientSession created successfully.")
            
            # Fetch Spot trading pairs
            logger.info("Fetching Spot trading pairs...")
            spot_url = "https://api.binance.com/api/v3/exchangeInfo"
            async with session.get(spot_url) as response:
                logger.info("Received response from Spot API.")
                spot_data = await response.json()  # Load JSON response
                spot_pairs = [symbol['symbol'] for symbol in spot_data['symbols']]
                logger.info(f"Fetched {len(spot_pairs)} Spot trading pairs.")

            # Fetch Futures trading pairs
            logger.info("Fetching Futures trading pairs...")
            futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            async with session.get(futures_url) as response:
                logger.info("Received response from Futures API.")
                futures_data = await response.json()
                futures_pairs = [symbol['symbol'] for symbol in futures_data['symbols']]
                logger.info(f"Fetched {len(futures_pairs)} Futures trading pairs.")

            # Fetch Perpetual trading pairs
            logger.info("Fetching Perpetual trading pairs...")
            perpetual_url = "https://dapi.binance.com/dapi/v1/exchangeInfo"
            async with session.get(perpetual_url) as response:
                logger.info("Received response from Perpetual API.")
                perpetual_data = await response.json()
                perpetual_pairs = [symbol['symbol'] for symbol in perpetual_data['symbols']]
                logger.info(f"Fetched {len(perpetual_pairs)} Perpetual trading pairs.")

            # Combine all pairs
            trading_pairs = list(set(spot_pairs + futures_pairs + perpetual_pairs))
            logger.info(f"Total trading pairs fetched: {len(trading_pairs)}")
            return trading_pairs
    except asyncio.TimeoutError:
        logger.error("Timeout while fetching trading pairs.")
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching trading pairs: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching trading pairs: {e}")
        raise

async def main():
    trading_pairs = await fetch_trading_pairs()
    if trading_pairs:
        logger.info(f"Trading pairs: {trading_pairs}")

if __name__ == "__main__":
    asyncio.run(main())

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
            account_url = "https://api.binance.com/api/v3/account"
            headers = {"X-MBX-APIKEY": self.api_key}  # Add your API key here
            async with session.get(account_url, headers=headers) as response:
                account_data = await response.json()
                maker_commission = account_data.get('makerCommission', 10) / 10000  # Convert to decimal (e.g., 10 -> 0.001)
                taker_commission = account_data.get('takerCommission', 15) / 10000  # Convert to decimal (e.g., 15 -> 0.0015)
                self.logger.info(f"Maker commission: {maker_commission}, Taker commission: {taker_commission}")

            # Fetch Spot trading pairs
            self.logger.info("Fetching Spot trading pairs...")
            spot_url = "https://api.binance.com/api/v3/exchangeInfo"
            async with session.get(spot_url) as response:
                self.logger.info("Received response from Spot API.")
                spot_data = await response.json()  # Load JSON response

                # Extract distinct quote currencies
                self.quote_currencies = set()

                # Store trading pair information in the database
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
                            'max_margin_allowed': 1000.0 if symbol_info['isMarginTradingAllowed'] else 0  # Example value
                        }

                        # Add quote currency to the set
                        self.quote_currencies.add(symbol_info['quoteAsset'].lower())

                        # Save trading pair information to the database
                        await self.save_trading_pair_to_db(trading_pair)

                self.logger.info(f"Distinct quote currencies: {self.quote_currencies}")

            # Exclude high-load pairs from the trading pairs list
            self.trading_pairs = list(set([symbol_info['symbol'].lower() for symbol_info in spot_data['symbols'] if symbol_info['status'] == 'TRADING']) - set(self.highload_pairs))
            self.logger.info(f"Total trading pairs fetched (excluding high-load pairs): {len(self.trading_pairs)}")
    except asyncio.TimeoutError:
        self.logger.error("Timeout while fetching trading pairs.")
    except aiohttp.ClientError as e:
        self.logger.error(f"Network error fetching trading pairs: {e}")
    except Exception as e:
        self.logger.error(f"Unexpected error fetching trading pairs: {e}")
        raise

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
                # Extract maker and taker commissions from the trading pair data
                maker_commission = trading_pair.get('makerCommission', 0)  # Default to 0 if not provided
                taker_commission = trading_pair.get('takerCommission', 0)  # Default to 0 if not provided

                # Set maximum margin allowed (if margin trading is enabled)
                max_margin_allowed = trading_pair.get('maxMarginAllowed', 0) if trading_pair['is_margin_trading_allowed'] else 0

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
                    maker_commission,
                    taker_commission,
                    max_margin_allowed
                ))
                await conn.commit()
                self.logger.info(f"Saved/updated trading pair {trading_pair['symbol']} in database.")
            except Exception as e:
                self.logger.error(f"Failed to save/update trading pair {trading_pair['symbol']} in database: {e}")
                await conn.rollback()
                