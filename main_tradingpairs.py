import asyncio
import signal
import os
import sys
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from tradingpairs_binance import TradingPairsFetcher

class CodeReloader(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(".py"):
            print(f"Detected change in {event.src_path}, restarting...")
            os.execv(sys.executable, [sys.executable] + sys.argv)

async def watch_files():
    observer = Observer()
    event_handler = CodeReloader()
    observer.schedule(event_handler, path=".", recursive=True)
    observer.start()
    
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        observer.stop()
        observer.join()

async def run_producer():
    producer = TradingPairsFetcher()
    try:
        producer.logger.info("run_producer TradingPairsFetcher will run now")
        await producer.run()
    except asyncio.CancelledError:
        producer.logger.info("Producer was cancelled.")
    except Exception as e:
        producer.logger.error(f"Unexpected error in producer: {e}")
    finally:
        await producer.cleanup()  # Ensure resources are cleaned up

async def main():    
    watchdog_task = asyncio.create_task(watch_files())
    consumer_tasks = asyncio.create_task(run_producer())
    all_tasks = watchdog_task + consumer_tasks

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


if __name__ == "__main__":
    fetcher = TradingPairsFetcher()
    asyncio.run(fetcher.run())