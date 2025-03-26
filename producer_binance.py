import asyncio
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
        self.dbhandler = DBHandler(self.config_db)  # Initialize DB handler
        self.symbol_data = defaultdict(lambda: {
            'q': 0,
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
    def PriceRounding(self, price):
        if 1 <= price < 1000:
            return round(price, 2)
        elif price >= 1000:
            return round(price, 1)
        else:
            return price
    
    async def calculate_qusd(self, symbol, quantity, price):
        """Calculate the quantity in USD for a given trade."""
        try:
            # Get the trading pair information
            trading_pair = self.trading_pairs.get(symbol)
            if not trading_pair:
                self.logger.warning(f"No trading pair found for symbol: {symbol}")
                return quantity
            # Get the quote currency's USD value
            quote_usd_price = self.quote_currencies.get(trading_pair['quote_asset'].lower(), 0.0)

            # Calculate quantity in USD
            qusd = self.PriceRounding(quantity * quote_usd_price)
            self.logger.info(f"{trading_pair['base_asset'].lower()}/{trading_pair['quote_asset'].lower()} q:{quantity} p:{price} usd:{quote_usd_price} qusd:{qusd}")    
            return qusd
        except Exception as e:
            self.logger.error(f"Error calculating quantity in USD for {symbol}: {e}")
            return quantity
        
    async def fetch_trades(self, symbols):
        while True:  # Keep trying to reconnect
            try:
                #stream_names = [f"{symbol.lower()}@trade" for symbol in symbols]
                stream_names = [f"{symbol.lower()}@trade" for symbol in symbols]
                uri = f"wss://stream.binance.com:9443/stream?streams={'/'.join(stream_names)}"
                #uri = f"wss://stream.binance.com:9443/ws/{'@trade/'.join(symbols).lower()}@trade"
                async with websockets.connect(
                    uri,
                    ping_interval=20,  # Send ping every 20 seconds
                    ping_timeout=10,   # Wait 10 seconds for a pong response
                ) as websocket:
                    self.logger.info(f"Connected to WebSocket for symbols: {symbols}")
                    while True:
                        try:
                            message = await websocket.recv()
                            data = json.loads(message)
                            if 'data' in data:  # Binance streams wrap data in a 'data' field
                                await self.process_trade(data['data'])
                            else:
                                self.logger.warning(f"Unexpected message format: {data}")
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.error(f"WebSocket connection closed: {e}")
                            await asyncio.sleep(3)  # Wait before reconnecting
                            break  # Exit the inner loop and reconnect
            except Exception as e:
                self.logger.error(f"WebSocket connection failed: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)  # Wait before reconnecting

    async def process_trade(self, trade):
        symbol = trade['s'].lower()
        data = self.symbol_data[symbol]
        price = float(trade['p']) 
        price = self.PriceRounding(float(trade['p']))
        quantity = self.PriceRounding(float(trade['q']) * price)
        qusd = 0
        # If this is a quote currency pair (e.g., BTCUSDT, ETHUSDT), update the quote currency price in USD
        quote_currency = None
        if symbol.endswith("usdt") and self.trading_pairs.get(symbol, None):  # Direct quote currency pair (e.g., btcusdt)
            # Extract the quote currency (e.g., "btc" from "btcusdt")
            quote_currency = self.trading_pairs.get(symbol, None)['base_asset']
            #self.logger.info(f"qc: {quote_currency}, isAvailable: {quote_currency in self.quote_assets}")
            if (quote_currency 
                and quote_currency.lower() in self.quote_assets 
                and quote_currency.lower()!= 'usdc'
                and quote_currency.lower()!= 'fdusd'
                ):
                quote_currency = quote_currency.lower()
                #self.logger.info(f"quote_currency {quote_currency} p: {price} sofar: {len(self.quote_currencies)}")
                if self.quote_currencies.get(quote_currency, None) == None:
                    self.logger.info(self.quote_currencies)

                self.quote_currencies[quote_currency] = price  # Update the btc price in USDT
                # Publish the latest USD price to Kafka cache topic
                #await self.producer.send(self.topicQuoteUSDCache, key=quote_currency.encode('utf-8'), value=price)
                #self.logger.info(f"Published USD price for {quote_currency}: {price}")
                # Calculate quantity in USD
        if (not symbol.endswith("usdt") 
        and not symbol.endswith("usdc") 
        and not symbol.endswith("fdusd") 
        and not symbol.endswith("try")):
            qusd = await self.calculate_qusd(symbol, quantity, price)            
        is_buyer_maker = trade['m']
        #self.logger.info(f" {symbol} p:{price}, q:{quantity}, qusd:{qusd}")
        if data['o'] is None:
            data['o'] = price
            data['st'] = trade['T']
        data['c'] = price
        data['q'] += quantity
        data['qusd'] += qusd
        data['h'] = max(data['h'], price) if data['h'] else price
        data['l'] = min(data['l'], price) if data['l'] else price
        data['et'] = trade['T']

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
                await self.producer.stop()
            except Exception as e:
                self.logger.warning(f"Error while stopping Kafka producer: {e}")
                
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
            self.logger.info(f"Waiting {secWait5}")
            await asyncio.sleep(secWait5)
                #await asyncio.sleep(5)  # Publish every 10 seconds
            #self.logger.info(self.symbol_data)
            for symbol, data in self.symbol_data.items():
                #self.logger.info(f"Published1 to Kafka topic binance.{symbol}: {data}")
                if data['st']:
                    topic = self.topicTradesRaw #f"binance.{symbol}"
                    key = symbol.encode('utf-8')  # Use trading pair symbol as the key                    
                    try:
                        await self.producer.send(topic, key=key, value=data)
                        self.logger.info(f"Publish {symbol}: {data}")
                    except Exception as e:
                        self.logger.error(f"Failed to publish data for {symbol}: {e}")
                    self.symbol_data[symbol] = {  # Reset data for the next window
                        'q': 0,
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

        # Run all tasks concurrently
        await asyncio.gather(*tasks)
