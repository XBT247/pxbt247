import asyncio
from decimal import Decimal, ROUND_DOWN
import json
import aiohttp
import datetime
from collections import defaultdict
from aiokafka import AIOKafkaProducer
import websockets
from base_binance import KafkaBase
from aiokafka.errors import KafkaError
from dbhandler import DBHandler

class KafkaProducerBinance(KafkaBase):
    def __init__(self):
        super().__init__()       
        self.quote_assets = set()
        self.trade_logs_buffer = []  # Ensure this is always initialized as a list
        self.dbhandler = DBHandler(self.config_db)  # Initialize DB handler
        self.symbol_data = defaultdict(lambda: {
            'q': 0,
            'qp': None, # Quote price in USD
            'qusd': 0,
            'qs': 0,
            'qb': 0,
            't': 0,
            'ts': 0,
            'tb': 0,
            'o': None,
            'c': None,
            'h': None,
            'l': None,
            'st': None,
            'et': None
        }) 
        self.trade_log = defaultdict(lambda: {
            's': '',
            'q': 0,
            'p': 0,
            'qp': None,
            'pu': None,
            'bm': 0
        })   
        self.active_connections = {}  # Track active WebSocket connections
        self.connection_attempts = defaultdict(int)  # Track connection attempts per symbol
        self.max_attempts = 5  # Maximum connection attempts before giving up temporarily
        self._initialized = False

    def PriceRounding1(self, price):
        if not isinstance(price, (int, float)):
            return price
            
        s = f"{price:.15f}"  # Convert to string with 15 decimal places
        
        if price >= 1000:
            # For prices ≥1000: keep 1 decimal
            dot_pos = s.find('.')
            return float(s[:dot_pos+2])
        elif 1 <= price < 1000:
            # For 1-1000: keep 3 decimals
            dot_pos = s.find('.')
            return float(s[:dot_pos+4])
        elif 0.001 <= price < 1:
            # For 0.001-1: keep 5 decimals
            dot_pos = s.find('.')
            return float(s[:dot_pos+6])
        elif price < 0.001:
            # For <0.001: keep 7 decimals
            dot_pos = s.find('.')
            return float(s[:dot_pos+8])
        return price
    
    def PriceRounding(self, price):
        if not isinstance(price, (int, float, Decimal)):
            return price
        
        # Lightweight check for common cases (avoids Decimal overhead)
        if price == 0:
            return 0.0
        if isinstance(price, int) and price >= 1000:
            return float(price)

        # Only use Decimal for problematic floats
        if isinstance(price, float) and not price.is_integer():
            price_dec = Decimal(str(price)).quantize(
                Decimal('0.1') if price >= 1000 else
                Decimal('0.001') if price >= 1 else
                Decimal('0.00001') if price >= 0.001 else
                Decimal('0.0000001') if price >= 0.00001 else
                Decimal('0.000000001') if price >= 0.0000001 else
                Decimal('0.00000000001') if price >= 0.000000001 else
                Decimal('0.0000000000001') if price >= 0.00000000001 else
                Decimal('0.000000000000001'),
                rounding=ROUND_DOWN
            )
            return float(price_dec)
        
        return float(price)
    
    async def calculate_qusd(self, symbol, quantity, price, is_buyer_maker):
        """Calculate the quantity in USD for a given trade."""
        try:
            # Get the trading pair information
            trading_pair = self.trading_pairs.get(symbol)
            if not trading_pair:
                self.logger.warning(f"No trading pair found for symbol: {symbol}")
                return quantity, None
            # Get the quote currency's USD value            
            quote_info = self.quote_currencies.get(trading_pair['quote_asset'].lower(), None)
            if not quote_info:
                return quantity, None
            quote_usd_price = quote_info["price"]
            isUSDTBased = quote_info["isUSDTBased"]
            if quote_usd_price == 0.0:
                return quantity, None
            # Calculate quantity in USD
            qusd = self.PriceRounding(quantity / quote_usd_price) if isUSDTBased else self.PriceRounding(quantity * quote_usd_price)
            #self.logger.info(f"{trading_pair['base_asset'].lower()}/{trading_pair['quote_asset'].lower()},{quantity},{price},{quote_usd_price},{qusd},{is_buyer_maker}")    
            
            self.trade_logs_buffer.append(f"s:{symbol},q:{quantity},p:{price},qp:{quote_usd_price},pu:{qusd},bm:{is_buyer_maker};")
            return qusd, quote_usd_price
        except Exception as e:
            self.logger.error(f"Error calculating quantity in USD for {symbol}: {e}")
            return quantity
        
    async def fetch_trades(self, symbols):
        connection_id = ",".join(sorted(symbols))  # Create unique ID for this connection
        while True:  # Keep trying to reconnect
            try:
                self.connection_attempts[connection_id] += 1
                if self.connection_attempts[connection_id] > self.max_attempts:
                    self.logger.warning(f"Max connection attempts reached for {connection_id}. Waiting before retrying...")
                    await asyncio.sleep(60)  # Wait longer before retrying
                    self.connection_attempts[connection_id] = 0  # Reset attempts
                    continue
                
                stream_names = [f"{symbol.lower()}@trade" for symbol in symbols]
                uri = f"wss://stream.binance.com:9443/stream?streams={'/'.join(stream_names)}"
                
                # Track this connection
                self.active_connections[connection_id] = {
                    'symbols': symbols,
                    'status': 'connecting',
                    'last_activity': datetime.datetime.now()
                }
                
                async with websockets.connect(
                    uri,
                    ping_interval=10,  # Send ping every 20 seconds
                    ping_timeout=5,   # Wait 10 seconds for a pong response
                ) as websocket:
                    self.active_connections[connection_id]['status'] = 'connected'
                    self.active_connections[connection_id]['last_activity'] = datetime.datetime.now()
                    self.logger.info(f"Connected to WebSocket for symbols: {symbols}")
                    
                    while True:
                        try:
                            message = await websocket.recv()
                            self.active_connections[connection_id]['last_activity'] = datetime.datetime.now()
                            data = json.loads(message)
                            if 'data' in data:  # Binance streams wrap data in a 'data' field
                                await self.process_trade(data['data'])
                            else:
                                self.logger.warning(f"Unexpected message format: {data}")
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.error(f"WebSocket connection closed for {connection_id}: {e}")
                            self.active_connections[connection_id]['status'] = 'disconnected'
                            await asyncio.sleep(3)  # Wait before reconnecting
                            break  # Exit the inner loop and reconnect
                        except Exception as e:
                            self.logger.error(f"Error in WebSocket connection for {connection_id}: {e}")
                            self.active_connections[connection_id]['status'] = 'error'
                            await asyncio.sleep(3)
                            break
            except Exception as e:
                self.logger.error(f"WebSocket connection failed for {connection_id}: {e}. Retrying in 10 seconds...")
                self.active_connections[connection_id]['status'] = 'error'
                await asyncio.sleep(10)  # Wait before reconnecting
            finally:
                if connection_id in self.active_connections:
                    self.active_connections[connection_id]['status'] = 'disconnected'

    async def monitor_connections(self):
        """Periodically check and restart failed connections."""
        while True:
            await asyncio.sleep(60)  # Check every 30 seconds
            
            # Log connection status
            active = sum(1 for conn in self.active_connections.values() if conn['status'] == 'connected')
            self.logger.info(f"Connection status: {active} active, {len(self.active_connections) - active} inactive")
            
            # Check for stale connections (no activity for 60 seconds)
            now = datetime.datetime.now()
            stale_connections = [
                conn_id for conn_id, conn in self.active_connections.items()
                if conn['status'] == 'connected' and (now - conn['last_activity']).total_seconds() > 60
            ]
            
            if stale_connections:
                self.logger.warning(f"Detected stale connections: {stale_connections}")
                # We can't directly restart here, but we can log and let the existing reconnection logic handle it

    async def process_trade(self, trade):
        symbol = trade['s'].lower()
        data = self.symbol_data[symbol]
        is_buyer_maker = trade['m']
        price = float(trade['p']) 
        price = self.PriceRounding(float(trade['p']))
        quantity = self.PriceRounding(float(trade['q']) * price)
        qusd = quantity
        quote_usd_price = None
        # If this is a quote currency pair (e.g., BTCUSDT, ETHUSDT), update the quote currency price in USD
        quote_currency = None
        isUSDTBased = False
        # eur(available) brl jpy try dai zar pln ars tusd cop
        if (self.trading_pairs.get(symbol, None)):  # Direct quote currency pair (e.g., btcusdt)
            # Extract the quote currency (e.g., "btc" from "btcusdt")
            quote_currency = self.trading_pairs.get(symbol, None)['base_asset']
            if (quote_currency == "usdt"):
                #self.logger.info(f"{symbol} qc: {quote_currency}, isAvailable: {quote_currency in self.quote_assets} USDT ")
                isUSDTBased = True
                quote_currency = self.trading_pairs.get(symbol, None)['quote_asset']

            if (quote_currency and quote_currency.lower() in self.quote_assets):
                quote_currency = quote_currency.lower()
                #self.logger.info(f"quote_currency {quote_currency} p: {price} sofar: {len(self.quote_currencies)}")
                if self.quote_currencies.get(quote_currency, None) == None:
                    self.logger.info(self.quote_currencies)
                # Update the btc price in USDT
                self.quote_currencies[quote_currency] = {
                    "price": price,
                    "isUSDTBased": isUSDTBased
                }
        if (not symbol.endswith("usdt") 
        and not symbol.endswith("usdc") 
        and not symbol.endswith("fdusd")):
            qusd, quote_usd_price = await self.calculate_qusd(symbol, quantity, price, 0 if is_buyer_maker else 1)            
        #self.logger.info(f" {symbol} p:{price}, q:{quantity}, qusd:{qusd}")
        if data['o'] is None:
            data['o'] = price
            data['st'] = trade['T']
        
        data['c'] = price
        data['qp'] = quote_usd_price
        data['q'] += quantity
        data['qusd'] += qusd
        data['h'] = max(data['h'], price) if data['h'] else price
        data['l'] = min(data['l'], price) if data['l'] else price
        data['et'] = trade['T']
        if qusd == 0:
            qusd = quantity
        if is_buyer_maker:
            data['qs'] += qusd
            data['ts'] += 1
            data['t'] += 1
        else:
            data['qb'] += qusd
            data['tb'] += 1
            data['t'] += 1
            
    async def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'producer') and self.producer:
            try:
                if not self.producer._closed:
                    # First flush any pending messages
                    try:
                        await asyncio.wait_for(
                            self.producer.flush(),
                            timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning("Timed out waiting for message flush")
                    await self.producer.stop()
                self._initialized = False
            except Exception as e:
                self.logger.error(f"Error stopping producer: {e}")
            finally:
                # Ensure producer reference is cleared
                del self.producer
                
    async def publish_to_kafka(self):
        self.logger.info(f"publish_to_kafka")
        while True:
            now = datetime.datetime.now() 
            # Calculate the number of seconds until the next 5th second
            current_second = now.second
            secWait5 = (5 - (current_second % 5)) % 5  # Correct calculation

            # If it's already the 5th second, wait for the next minute
            if secWait5 == 0:
                secWait5 = 5            
            if(now.minute % 5 ==0 and now.second == 0):
                self.logger.info(f"{now.minute}")

            await asyncio.sleep(secWait5)
                #await asyncio.sleep(5)  # Publish every 10 seconds
            #self.logger.info(self.symbol_data)
            
            if self.trade_logs_buffer:
                try:
                    await self.producer.send(
                        self.topicTradesLog,
                        key="NoKey".encode('utf-8'),
                        value="|".join(self.trade_logs_buffer)
                    )
                except Exception as e:
                    self.logger.error(f"Failed to publish trade logs {self.topicTradesLog}: {e}")
                finally:
                    self.trade_logs_buffer.clear()

            reset_symbols = []
            for symbol, data in self.symbol_data.items():
                #self.logger.info(f"Published1 to Kafka topic binance.{symbol}: {data}")
                if data['st'] and data['t'] > 3:                    
                    key = symbol.encode('utf-8')  # Use trading pair symbol as the key                    
                    try:
                        await self.producer.send(self.topicTradesRaw, key=key, value=data)
                        #self.logger.info(f"Publish {symbol}: {data}")
                        reset_symbols.append(symbol)
                    except Exception as e:
                        self.logger.error(f"Failed to publish data for {symbol}: {e}")
                    
            # Reset after iteration complete
            for symbol in reset_symbols:
                try:
                    self.symbol_data[symbol] = {  # Reset data for the next window
                            'q': 0,
                            'qp': None,
                            'qusd': 0,
                            'qs': 0,
                            'qb': 0,
                            't': 0,
                            'ts': 0,
                            'tb': 0,
                            'o': None,
                            'c': None,
                            'h': None,
                            'l': None,
                            'st': None,
                            'et': None
                        }
                except Exception as e:
                    self.logger.error(f"Error resetting symbol data for {symbol}: {e}")

    async def initialize_producer(self, retries=3, delay=5):
        """Initialize producer with retry logic"""
        for attempt in range(retries):
            try:
                if hasattr(self, 'producer') and self.producer:
                    await self.cleanup()
                    
                self.producer = await super().create_kafka_producer()
                self._initialized = True
                self.logger.info("Producer initialized successfully")
                return self.producer
            except Exception as e:
                self.logger.error(f"Initialization attempt {attempt+1} failed: {str(e)}")
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(delay)

    async def check_producer_health(self):
        """Robust health check compatible with all aiokafka versions"""
        if not hasattr(self, 'producer') or self.producer is None:
            return False, "Producer not initialized"

        try:
            # Basic structural check
            if self.producer._closed:
                return False, "Producer closed"

            # Version-agnostic operational check
            try:
                # Try both possible client access methods
                client = getattr(self.producer.client, '_client', None) or self.producer.client
                if client is None:
                    return False, "Client not available"
                
                # Test connection with simple metadata request
                await asyncio.wait_for(
                    client.list_topics(),
                    timeout=5.0
                )
                return True, "Healthy"
            except AttributeError:
                # Fallback check if internal structure changes
                if hasattr(self.producer, 'client') and not self.producer._closed:
                    return True, "Healthy (basic check)"
                return False, "Client structure changed"

        except asyncio.TimeoutError:
            return False, "Connection timeout"
        except Exception as e:
            return False, f"Health verification failed: {str(e)}"
        
    async def is_kafka_connected(self):
        if not hasattr(self, 'producer') or self.producer is None:
            return False
        try:
            # Simple metadata request to verify connection
            await self.producer.client._client.list_topics()
            return True
        except Exception:
            return False
           
    async def run(self):
        """Main method to run the producer."""
        # Fetch trading pairs initially
        self.logger.info(f"Producer starting RUN ")
                   
        # Wait for Kafka to be ready
        if not await self.wait_for_kafka():
            self.logger.error("Exiting due to Kafka initialization failure.")
            raise
        
        try:
            await self.dbhandler.create_pool()  # Create database connection pool
            self.trading_pairs = await self.dbhandler.fetch_trading_pairs()
            self.quote_assets = list({pair['quote_asset'] for pair in self.trading_pairs.values()})
            self.logger.info(f"Trading pairs loaded: {len(self.trading_pairs)}, quote_assets: {len(self.quote_assets)}")
            #self.logger.info(f"Trading Pairs loaded: {len(self.trading_pairs)}, {self.trading_pairs}")
            #self.logger.info(f"Quote Assets loaded: {len(self.quote_assets)}, {self.quote_assets}")
        except Exception as e:
            self.logger.error(f"Failed to fetch trading pairs")
            raise
        # ✅ Filter out highload pairs before batching
        filtered_trading_pairs = [
            symbol for symbol in self.trading_pairs if symbol not in self.highload_pairs
        ]
        self.logger.info(f"filtered from highload_pairs: {len(filtered_trading_pairs)}")
        
        # Create Kafka producer
        self.producer = await self.create_kafka_producer()
        # Start fetching trades
        tasks = []
        # Create separate WebSocket connections for high-load pairs
        for pair in self.highload_pairs:
            tasks.append(self.fetch_trades([pair]))
        
        # Create WebSocket connections for batches of trading pairs
        batch_size = 50  # Adjust batch size as needed
        for i in range(0, len(filtered_trading_pairs), batch_size):
            batch = filtered_trading_pairs[i:i + batch_size]
            tasks.append(self.fetch_trades(batch))
        
        # Start publishing to Kafka
        tasks.append(self.publish_to_kafka())
        
        # Start connection monitoring
        tasks.append(self.monitor_connections())

        # Run all tasks concurrently
        await asyncio.gather(*tasks)