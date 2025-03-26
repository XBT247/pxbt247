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

async def main():
    stop_event = asyncio.Event()

    # Start producer as a continuous task
    taskP = asyncio.create_task(run_producer())
    all_tasks = [taskP]

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
