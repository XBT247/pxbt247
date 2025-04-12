import asyncio
import signal
from producer_binance import KafkaProducerBinance
from tradingpairs_binance import TradingPairsFetcher

async def run_producer():
    producer = KafkaProducerBinance()
    try:
        producer.logger.info("run_producer will run now")
        await producer.run()
    except asyncio.CancelledError:
        producer.logger.info("Producer was cancelled.")
    except Exception as e:
        producer.logger.error(f"Unexpected error in producer: {e}")

async def health_check(producer):
    """Periodically check the health of the Kafka producer."""
    while True:
        try:
            if producer.producer is not None and producer.producer._closed is False:
                producer.logger.info("Kafka producer is healthy.")
            else:
                producer.logger.error("Kafka producer is not healthy.")
        except Exception as e:
            producer.logger.error(f"Health check failed: {e}")
        await asyncio.sleep(30)  # Check health every 30 seconds

async def main():
    stop_event = asyncio.Event()

    # Start producer as a continuous task
    producer = KafkaProducerBinance()
    taskP = asyncio.create_task(run_producer())
    health_task = asyncio.create_task(health_check(producer))
    all_tasks = [taskP, health_task]

    # Handle graceful shutdown
    def handle_shutdown(*args):
        print("Received shutdown signal, cancelling tasks...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    await stop_event.wait()
    
    for task in all_tasks:
        task.cancel()
    await asyncio.gather(*all_tasks, return_exceptions=True)
    print("Shutdown complete.")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
