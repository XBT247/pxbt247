import asyncio
import signal
import sys
from maincontainers.tradingpairs_container import TradingPairsContainer

async def main():
    container = TradingPairsContainer("binance")
    try:
        await container.initialize()
        await container.run()
    finally:
        await container.shutdown()

def run():
    if sys.platform == "win32":
        # Windows-specific handling
        asyncio.run(main())
    else:
        # Unix-based systems
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def handle_shutdown(sig):
            logger.info(f"Received signal {sig.name}, shutting down...")
            tasks = asyncio.all_tasks(loop=loop)
            for task in tasks:
                task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_shutdown, sig)

        try:
            loop.run_until_complete(main())
        except asyncio.CancelledError:
            logger.info("Service stopped gracefully")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            loop.close()
            logger.info("Service shutdown complete")

if __name__ == "__main__":
    run()