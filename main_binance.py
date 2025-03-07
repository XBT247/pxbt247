import asyncio
from producer_binance import KafkaProducerBinance
from consumer_binance import KafkaConsumerBinance

async def health_check(producer):
    while True:
        await asyncio.sleep(60)  # Check every 60 seconds
        if not producer.producer:
            producer.producer = producer.create_kafka_producer()
            producer.logger.info("Restarted Kafka Producer")

async def main():
    producer = KafkaProducerBinance()
    consumer = KafkaConsumerBinance()

    # Run Producer, Consumer, and Health Check
    await asyncio.gather(
        producer.run(),
        consumer.run(),
        health_check(producer)
    )

if __name__ == "__main__":
    asyncio.run(main())