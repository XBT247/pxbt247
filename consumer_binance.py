import asyncio
import json
from base_binance import KafkaBase
from trendaware import TrendAware

class KafkaConsumerBinance(KafkaBase):
    def __init__(self):
        super().__init__()
        self.topics = self.config["trading_pairs"]
        self.consumer = self.create_kafka_consumer(self.topics)

    async def insert_into_db(self, symbol, data):
        query = f"""
            INSERT INTO tbl_binance_{symbol.lower()} 
            (qs, qb, ts, tb, o, c, h, l, st, et)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        async with await self.create_db_connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(query, (
                        data['qs'], data['qb'], data['ts'], data['tb'],
                        data['o'], data['c'], data['h'], data['l'],
                        data['st'], data['et']
                    ))
                    await conn.commit()
                    self.logger.info(f"Inserted data for {symbol}")
                except Exception as e:
                    self.logger.error(f"Failed to insert data for {symbol}: {e}")
                    await conn.rollback()

    async def run(self):
        for message in self.consumer:
            symbol = message.topic
            data = message.value
            await self.insert_into_db(symbol, data)
            self.consumer.commit()