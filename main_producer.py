from producer_binance import KafkaProducerBinance
import asyncio

async def run_producer():
    producer = KafkaProducerBinance()
    await producer.run()
    #await producer.fetch_trading_pairs()

if __name__ == "__main__":
    asyncio.run(run_producer())