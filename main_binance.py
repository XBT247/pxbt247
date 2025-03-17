import asyncio
from producer_binance import KafkaProducerBinance
from consumer_binance import KafkaConsumerBinance

NUM_CONSUMERS = 3

async def health_check(producer):
    while True:
        await asyncio.sleep(60)  # Check every 60 seconds
        if not producer.producer:
            producer.producer = producer.create_kafka_producer()
            producer.logger.info("Restarted Kafka Producer")

async def run_producer():
    producer = KafkaProducerBinance()
    await producer.run()

async def run_consumer(consumer_id, group_id):
    consumer = KafkaConsumerBinance(consumer_id=consumer_id, group_id=group_id)
    await consumer.run()

async def main():
    # Run Producer and Consumer in separate event loops
    group_id="trading-consumers"
    consumer_tasks = [asyncio.create_task(run_consumer(i, group_id)) for i in range(NUM_CONSUMERS)]
    producer_task = asyncio.create_task(run_producer())
    #consumer_task = asyncio.create_task(run_consumer())
    await asyncio.gather(producer_task, *consumer_tasks)

if __name__ == "__main__":
    asyncio.run(main())