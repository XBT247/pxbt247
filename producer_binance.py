import asyncio
import json
import aiohttp
import datetime
from collections import defaultdict
import websockets
from base_binance import KafkaBase

class KafkaProducerBinance(KafkaBase):
    def __init__(self):
        super().__init__()
        self.trading_pairs = []
        self.batch_size = self.config["batch_size"]
        self.producer = self.create_kafka_producer()
        self.symbol_data = defaultdict(lambda: {
            'qs': 0,
            'qb': 0,
            'ts': 0,
            'tb': 0,
            'o': None,
            'c': None,
            'h': None,
            'l': None,
            'st': None,
            'et': None
        })
    async def PriceRounding(price):    
        if(price >= 1 & price < 300):
            return round(price, 2)
        if(price >= 300):
            return round(price, 1)
        else:
            return price
        
    
    async def fetch_trading_pairs(self):
        """Fetch trading pairs from Binance (Spot, Futures, Perpetual)."""
        async with aiohttp.ClientSession() as session:
            # Fetch Spot trading pairs
            spot_url = "https://api.binance.com/api/v3/exchangeInfo"
            async with session.get(spot_url) as response:
                spot_data = await response.json()
                spot_pairs = [symbol['symbol'] for symbol in spot_data['symbols']]

            # Fetch Futures trading pairs
            futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            async with session.get(futures_url) as response:
                futures_data = await response.json()
                futures_pairs = [symbol['symbol'] for symbol in futures_data['symbols']]

            # Fetch Perpetual trading pairs
            perpetual_url = "https://dapi.binance.com/dapi/v1/exchangeInfo"
            async with session.get(perpetual_url) as response:
                perpetual_data = await response.json()
                perpetual_pairs = [symbol['symbol'] for symbol in perpetual_data['symbols']]

            # Combine all pairs
            self.trading_pairs = sorted(set(spot_pairs + futures_pairs + perpetual_pairs))
            self.logger.info(f"Fetched {len(self.trading_pairs)} trading pairs from Binance.")

            # Save trading pairs to database
            await self.save_trading_pairs_to_db()

    async def save_trading_pairs_to_db(self):
        """Save trading pairs to the database table `tbl_coins`."""
        query = """
            INSERT INTO tbl_coins (symbol, created_at)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE symbol=symbol
        """
        async with await self.create_db_connection() as conn:
            async with conn.cursor() as cursor:
                for symbol in self.trading_pairs:
                    try:
                        await cursor.execute(query, (symbol, datetime.datetime.now()))
                        await conn.commit()
                    except Exception as e:
                        self.logger.error(f"Failed to save symbol {symbol} to database: {e}")
                        await conn.rollback()

    async def fetch_trades(self, symbols):
        while True:  # Keep trying to reconnect
            try:
                uri = f"wss://stream.binance.com:9443/ws/{'@trade/'.join(symbols).lower()}@trade"
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=10
                ) as websocket:
                    self.logger.info(f"Connected to WebSocket for symbols: {symbols}")
                    while True:
                        try:
                            message = await websocket.recv()
                            data = json.loads(message)
                            await self.process_trade(data)
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.error(f"WebSocket connection closed: {e}")
                            break  # Exit the inner loop and reconnect
            except Exception as e:
                self.logger.error(f"WebSocket connection failed: {e}. Retrying in 10 seconds...")
                await asyncio.sleep(10)  # Wait before reconnecting

    async def process_trade(self, trade):
        symbol = trade['s']
        data = self.symbol_data[symbol]
        price = float(trade['p'])
        quantity = float(trade['q'])
        is_buyer_maker = trade['m']

        if data['o'] is None:
            data['o'] = price
            data['st'] = trade['T']
        data['c'] = price
        data['h'] = max(data['h'], price) if data['h'] else price
        data['l'] = min(data['l'], price) if data['l'] else price
        data['et'] = trade['T']

        if is_buyer_maker:
            data['qs'] += quantity
            data['ts'] += 1
        else:
            data['qb'] += quantity
            data['tb'] += 1

    async def publish_to_kafka(self):
        while True:
            await asyncio.sleep(5)  # Publish every 10 seconds
            for symbol, data in self.symbol_data.items():
                if data['st']:
                    self.producer.send(symbol, value=data)
                    self.logger.info(f"Published to Kafka topic {symbol}: {data}")
                    self.symbol_data[symbol] = {  # Reset data for the next window
                        'qs': 0,
                        'qb': 0,
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
        # Fetch trading pairs initially
        await self.fetch_trading_pairs()

        # Schedule trading pairs update every hour
        asyncio.create_task(self.schedule_trading_pairs_update())

        # Start fetching trades
        tasks = []
        for i in range(0, len(self.trading_pairs), self.batch_size):
            batch = self.trading_pairs[i:i + self.batch_size]
            tasks.append(self.fetch_trades(batch))
        tasks.append(self.publish_to_kafka())
        await asyncio.gather(*tasks)

    async def schedule_trading_pairs_update(self):
        """Update trading pairs every hour."""
        while True:
            await asyncio.sleep(3600)  # Wait for 1 hour
            await self.fetch_trading_pairs()