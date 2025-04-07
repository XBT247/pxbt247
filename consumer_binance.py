from collections import OrderedDict
import json
import logging
import asyncio
from aiokafka.errors import KafkaError
from kafka import KafkaAdminClient
from base_binance import KafkaBase
from dbhandler import DBHandler
from trend_aware import TrendAware
import aiomysql

class KafkaConsumerBinance(KafkaBase):
    def __init__(self, consumer_id, group_id="cgRawTrades"):
        super().__init__()
        self.dbhandler = DBHandler(self.config_db)  # Initialize DB handler
        self.consumer_id = consumer_id
        self.group_id = group_id
        self.local_cache = OrderedDict()  # ✅ LRU Cache to manage memory
        # Initialize the AIOKafkaConsumer with an empty topic list (will be updated later)
        self.consumer = None
        self.consumerCache = None
        self.producer = None
        self.logger.info("Consumer {consumer_id} initialized with aiokafka.")

    async def create_table_if_not_exists(self, symbol):
        """Create a table in MySQL if it does not exist, using the schema of an existing table."""
        async with await self.create_db_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    # Use the schema of an existing table (e.g., tbl_binance_template)
                    await cursor.execute(f"""
                        CREATE TABLE tbl_binance_{symbol.lower()} LIKE tbl_binance_template
                    """)
                    await conn.commit()
                    self.logger.info(f"Table 'tbl_binance_{symbol.lower()}' created successfully.")
                except Exception as e:
                    self.logger.error(f"Failed to create table 'tbl_binance_{symbol.lower()}': {e}")
                    await conn.rollback()

    async def insert_into_db(self, symbol, data):
        """Insert data into the database, creating the table if it does not exist."""
        query = f"""
            INSERT INTO tbl_binance_{symbol.lower()} 
            (seller_qty, buyer_qty, seller_trades, buyer_trades, open_price, close_price, high_price, low_price, start_time, end_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        async with await self.create_db_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, (
                        data['seller_qty'], data['buyer_qty'], data['seller_trades'], data['buyer_trades'],
                        data['open_price'], data['close_price'], data['high_price'], data['low_price'],
                        data['start_time'], data['end_time']
                    ))
                    await conn.commit()
                    self.logger.info(f"Inserted data for {symbol}")
                except Exception as e:
                    # Check if the exception is related to a missing table
                    if "Table" in str(e) and "doesn't exist" in str(e):
                        self.logger.info(f"Table 'tbl_binance_{symbol.lower()}' does not exist. Creating it...")
                        await self.create_table_if_not_exists(symbol)
                        # Retry the insertion after creating the table
                        await cursor.execute(query, (
                            data['seller_qty'], data['buyer_qty'], data['seller_trades'], data['buyer_trades'],
                            data['open_price'], data['close_price'], data['high_price'], data['low_price'],
                            data['start_time'], data['end_time']
                        ))
                        await conn.commit()
                        self.logger.info(f"Inserted data for {symbol} after creating the table.")
                    else:
                        self.logger.error(f"Failed to insert data for {symbol}: {e}")
                        await conn.rollback()

    async def get_cached_object(self, symbol):
        """Fetch cached object from in-memory LRU cache."""
        return self.local_cache.get(symbol, None)

    async def update_cache(self, symbol, cached_object):
        """Store or update the cache with the new processed object."""
        if len(self.local_cache) > 5000:  # ✅ Limit cache size
            self.local_cache.popitem(last=False)  # ✅ Remove the oldest entry
        self.local_cache[symbol] = cached_object

    async def process_trade_data(self, symbol, trade_data):
        """Retrieve cached object, update trade data, and re-cache it."""
        cached_object = await self.get_cached_object(symbol)
        if not cached_object:
            self.logger.info(f"Cache not found {symbol}. Initializing...")
            cached_object = TrendAware(self.awareMed)  # Initialize if not found

        # ✅ Append new trade and compute indicators (dummy calculations here)
        cached_object["trades"].append(trade_data)
        cached_object["rsi"] = sum(t["price"] for t in cached_object["trades"]) / len(cached_object["trades"])
        cached_object["macd"] = cached_object["rsi"] * 0.5
        cached_object["ma20"] = cached_object["rsi"] * 1.2

        # ✅ Update cache and send updated object to Kafka
        await self.update_cache(symbol, cached_object)
        await self.producer.send_and_wait(self.cacheTrendaware, key=symbol.encode(), value=json.dumps(cached_object).encode())
        self.logger.info(f"Consumer for {symbol} processed successfully.")

    async def run(self):
        
        # Wait for Kafka to be ready
        if not await self.wait_for_kafka():
            self.logger.error("Exiting due to Kafka initialization failure.")
            return
        self.consumer = await self.create_kafka_consumer(self.topicTradesRaw, self.group_id)
        self.consumerCache = self.create_kafka_consumer(self.cacheTrendaware, "cache-reader")  # ✅ Create it once
        self.producer = await self.create_kafka_producer()
        """Main consumer loop that processes raw trade data in batches."""
        await self.consumer.start()
        await self.producer.start()
        # ✅ Start cache listener asynchronously
        asyncio.create_task(self.cache_listener())
        try:
            while True:
                batch = await self.consumer.getmany(timeout_ms=100, max_records=50)
                tasks = []
                for tp, messages in batch.items():
                    for message in messages:
                        symbol = message.key.decode('utf-8')  # Trading pair symbol is the key
                        trade_data = json.loads(message.value.decode())
                        tasks.append(self.process_trade_data(symbol, trade_data))

                # ✅ Process all messages in parallel
                await asyncio.gather(*tasks)

                # ✅ Commit offsets after batch processing
                await self.consumer.commit()
                await asyncio.sleep(0.01)
        finally:
            await self.consumer.stop()
            await self.producer.stop()

    async def cache_listener(self):
        """Listens to the cache topic and updates local cache periodically."""
        await self.consumerCache.start()
        try:
            while True:
                batch = await self.consumerCache.getmany(timeout_ms=500, max_records=50)
                for tp, messages in batch.items():
                    for msg in messages:
                        symbol = msg.key.decode()
                        self.local_cache[symbol] = json.loads(msg.value.decode())
                await asyncio.sleep(0.1)  # ✅ Prevents CPU overload

        finally:
            await self.consumerCache.stop()

"""
    def SetTrendAware(symbol, trade):
        global cacheKeyCandleInsert, awareShort, avgLimit
        try:
            thisTrade = None
            ptsQtyPlus = PointsQty(trade['buy_qty'])
            ptsQtyMinus = PointsQty(trade['sell_qty'])
            ptsTradesPlus = PointsTrade(trade['buy_trades'])
            ptsTradesMinus = PointsTrade(trade['sell_trades'])

            thisTrade = {
                "serverTime": trade['endTime'],
                "open": trade['open_price'],
                "close": trade['close_price'],
                "volume": (trade['buy_qty'] + trade['sell_qty']),
                "totalVolumeBuy": trade['buy_qty'],
                "totalVolumeSell": trade['sell_qty'],
                "trades": (trade['buy_trades'] + trade['sell_trades']),
                "totalTradesBuy": trade['buy_trades'],
                "totalTradesSell": trade['sell_trades'],
                "volumePoints": (ptsQtyPlus - ptsQtyMinus),
                "tradesPoints": (ptsTradesPlus - ptsTradesMinus)
            }

            aware25 = TrendAware(awareShort, avgLimit=25, showLog=False, v=2, posClosingDelay=False, posClosingBand=False, showTable=False, tierCommission=0.075)
            aware25.AddTrade(thisTrade)
            avgQtr100 = sum(aware25.h100s) / len(aware25.h100s)
            rowData = [
                trade['endTime'],
                0,
                aware25.thisTrade["open"],
                aware25.thisTrade["close"],
                aware25.thisTrade["avgPrice"],
                trade['highest_price'],
                trade['lowest_price'],
                trade['total_qty'],
                trade['total_trades'],
                trade['buy_qty'],
                trade['sell_qty'],
                None,
                None,
                None,
                trade['buy_trades'],
                trade['sell_trades'],
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                aware25.thisTrade["volumePoints"],
                aware25.thisTrade["tradesPoints"],
                None,
                None,
                aware25.thisTrade['qtr_history30'],
                aware25.thisTrade['qtr_history100'],
                avgQtr100,
                None,
                None,
                None,
                None,
                1 if aware25.thisTrade["isSidelined"] else 0,
                None,
                aware25.thisTrade["high200"],
                aware25.thisTrade["low200"]
            ]
            CacheRowInsert("tbl_" + symbol + "_BINANCE", rowData, cacheKeyCandleInsert)
            if not hasattr(self, 'lastTrade') or not self.lastTrade:
                self.lastTrade = aware25.thisTrade
                return
            self.lastTrade = thisTrade
            if strLog.strip() != "":
                CacheRowInsert("tbllogsb", [strLog, '30', end_time_str], cacheKeyLogInsert)

            aware25.AddLastTrade(lastTrade)
        closeDiffIndex25 = aware25.AddPriceDiffs(closeDiffIndex25)
        countArr = len(aware25.avgPriceDistance)
        if countArr > 0:
            dirPriceWRTAvg = aware25.avgPriceDistance[-1]["dir"]
            thisCountPriceWRTAvg = aware25.avgPriceDistance[-1]["count"]
        aware25.ta2()
        aware25.AddLastSumPosNeg()
        aware25.ta3(closeDiffIndex25)
        trendHighLow25 = aware25.SetTrendColor(dirPriceWRTAvg)
        aware25.SetBand(dirPriceWRTAvg)
        
        if trendHighLow25:
            c25Last = trendHighLow25[-1]
            if len(trendHighLow25) > 1:
                c25Last2 = trendHighLow25[-2]
            if len(trendHighLow75) > 2 and len(trendHighLow25) > 2:
                c25Last3 = trendHighLow25[-3]
        
        if c25Last['count'] == 1 and 'c25Last2' in locals():
            colorCode = 0
            if c25Last2['color'] == cw:
                colorCode = 1
            elif c25Last2['color'] == cy:
                colorCode = 2
            elif c25Last2['color'] == cg:
                colorCode = 3
            elif c25Last2['color'] == ca:
                colorCode = 4
            rowDataCC = [25, colorCode, c25Last2['open'], c25Last2['close'], c25Last2['high'], c25Last2['low'], c25Last2['startAvgPrice'], c25Last2['endAvgPrice'], c25Last2['startServerTime'].strftime(format), (None if not c25Last2['endServerTime'] or c25Last2['endServerTime'] == '' else c25Last2['endServerTime'].strftime(format)), c25Last2['count'], c25Last2['countdirPriceUp'], c25Last2['countdirPriceDown'], c25Last2['countAboveAvgPrice'], c25Last2['colorParent'], c25Last2['colorParents'], c25Last2['isSidelined'], '', '']
            CacheRowInsert(f"tbl_{symbol}_BINANCE_CC", rowDataCC, cacheKeyCandleCCInsert)
        
        lastPriceDistance = aware25.PriceMaxDistanceFromAvgPrice(lastPriceDistance, c25Last['color'])
        except Exception as ex:
            DBInsertRow(sqlLogInsert, [ex.message, '31', end_time_str])
    def PointsQty(quantity):
        global isSidelined
        pts = 0
        if isSidelined:
            if quantity < 1000:
                pts = 0
            elif quantity < 30000:
                pts = 0.5
            elif quantity < 65000:
                pts = 1
            elif quantity < 150000:
                pts = 1.5
            elif quantity < 350000:
                pts = 2
            elif quantity < 400000:
                pts = 3
            elif quantity < 600000:
                pts = 4
            elif quantity < 800000:
                pts = 5
            elif quantity < 1000000:
                pts = 6
            elif quantity < 1250000:
                pts = 7
            elif quantity < 1500000:
                pts = 8
            elif quantity < 2000000:
                pts = 9
            elif quantity < 2500000:
                pts = 10
            elif quantity < 3500000:
                pts = 11
            elif quantity < 4500000:
                pts = 12
            else:
                pts = 13
        else:
            if quantity < 2000:
                pts = 0
            elif quantity < 60000:
                pts = 0.5
            elif quantity < 130000:
                pts = 1
            elif quantity < 300000:
                pts = 1.5
            elif quantity < 600000:
                pts = 2
            elif quantity < 900000:
                pts = 3
            elif quantity < 1250000:
                pts = 4
            elif quantity < 1500000:
                pts = 5
            elif quantity < 1800000:
                pts = 6
            elif quantity < 2200000:
                pts = 7
            elif quantity < 2800000:
                pts = 8
            elif quantity < 4000000:
                pts = 9
            elif quantity < 5000000:
                pts = 10
            elif quantity < 7000000:
                pts = 11
            elif quantity < 9000000:
                pts = 12
            else:
                pts = 13
        return pts

    async def get_matching_topics(self):
        "" "Fetch all topics from Kafka matching the pattern 'binance.*'." ""
        while True:
            try:
                # Use KafkaAdminClient to list topics
                admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_config['bootstrap_servers'])
                all_topics = admin_client.list_topics()
                admin_client.close()
                self.logger.info(f"Fetched topics from Kafka: {all_topics}")

                # Filter topics matching the pattern 'binance.*'
                matching_topics = [topic for topic in all_topics if topic.startswith("binance.")]
                if matching_topics:
                    self.logger.info(f"Subscribing to topics: {matching_topics}")
                    return matching_topics
                else:
                    self.logger.info("No matching topics found. Waiting for Producer to create topics...")
                    await asyncio.sleep(10)  # Wait for 10 seconds before retrying
            except KafkaError as e:
                self.logger.error(f"Failed to fetch topics from Kafka: {e}")
                await asyncio.sleep(10)  # Wait before retrying"
                ""
"""