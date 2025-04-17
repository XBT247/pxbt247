import logging
logging.basicConfig(level=logging.INFO)  # Safety net
import asyncio
import signal
from tradingpairs.tradingpairs_binance import TradingPairsFetcher

async def run_tradingpairsfetcher():
    producerTP = TradingPairsFetcher()
    try:
        producerTP.logger.info("run_tradingpairsfetcher TradingPairsFetcher will run now")
        await producerTP.run()
    except asyncio.CancelledError:
        producerTP.logger.info("producerTP was cancelled.")
    except Exception as e:
        producerTP.logger.error(f"Unexpected error in producerTP: {e}")
    finally:
        producerTP.logger.info(" cleanup.")
        await producerTP.cleanup()  # Ensure resources are cleaned up

async def periodic_task(interval, stop_event):
    """Runs `run_tradingpairsfetcher` immediately and then every `interval` seconds."""
    await run_tradingpairsfetcher()  # Run immediately
    while not stop_event.is_set():
        await asyncio.sleep(interval)  # Wait for the interval
        await run_tradingpairsfetcher()  # Run again

async def main():    
    # Start TradingPairsFetcher every hour (3600 seconds)
    stop_event = asyncio.Event()
    taskT = asyncio.create_task(periodic_task(3600, stop_event))
    all_tasks = [taskT]

    # Handle graceful shutdown
    stop_event = asyncio.Event()
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
