import asyncio
import signal
from consumer_binance import KafkaConsumerBinance

async def run_consumer(consumer_id, group_id):
    consumer = KafkaConsumerBinance(consumer_id=consumer_id, group_id=group_id)
    await consumer.run()

async def main():
    group_id = "cgRawTrades"
    NUM_CONSUMERS = 1
    consumer_tasks = [asyncio.create_task(run_consumer(i, group_id)) for i in range(NUM_CONSUMERS)]

    # Handle graceful shutdown
    stop_event = asyncio.Event()
    def handle_shutdown(*args):
        print("Received shutdown signal, cancelling tasks...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    await stop_event.wait()

    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    print("Shutdown complete.")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
