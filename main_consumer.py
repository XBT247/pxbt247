from consumer_binance import KafkaConsumerBinance
import asyncio

async def run_consumer(consumer_id, group_id):
    consumer = KafkaConsumerBinance(consumer_id=consumer_id, group_id=group_id)
    await consumer.run()

if __name__ == "__main__":
    NUM_CONSUMERS = 3  # Number of consumer instances
    group_id = "trading-consumers"
    asyncio.run(asyncio.gather(*[run_consumer(i, group_id) for i in range(NUM_CONSUMERS)]))