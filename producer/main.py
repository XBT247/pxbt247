import asyncio
import signal
from producer_binance import KafkaProducerBinance
from tradingpairs.tradingpairs_binance import TradingPairsFetcher

async def run_producer(producer):
    try:
        producer.logger.info("Initializing producer...")
        
        # Use the new initialization method
        await producer.initialize_producer()
        
        # Run main logic
        await producer.run()
    except asyncio.CancelledError:
        producer.logger.info("Producer was cancelled.")
    except Exception as e:
        producer.logger.error(f"Unexpected error in producer: {e}")
    finally:
        if hasattr(producer, 'producer') and producer.producer:
            await producer.cleanup()

async def health_check(producer):
    """Enhanced health monitoring with state tracking"""
    last_state = None
    while True:
        try:
            healthy, reason = await producer.check_producer_health()
            current_state = (healthy, reason)
            
            # Only log state changes
            if current_state != last_state:
                if healthy:
                    producer.logger.info(f"Kafka producer healthy: {reason}")
                else:
                    producer.logger.warning(f"Kafka producer issue: {reason}")
                last_state = current_state

            # Additional WebSocket monitoring
            active_ws = len([c for c in producer.active_connections.values() 
                           if c.get('status') == 'connected'])
            if active_ws < len(producer.active_connections):
                producer.logger.warning(f"WebSocket status: {active_ws}/{len(producer.active_connections)} active")
                
        except Exception as e:
            producer.logger.error(f"Health monitoring error: {e}", exc_info=True)
            last_state = None
            
        await asyncio.sleep(30)

async def main():
    stop_event = asyncio.Event()
    producer = KafkaProducerBinance()

    # Initialize producer first
    try:
        producer.producer = await producer.create_kafka_producer()
    except Exception as e:
        producer.logger.error(f"Failed to initialize Kafka producer: {e}")
        return
    # Add shutdown timeout
    async def shutdown():
        print("Starting graceful shutdown...")
        await asyncio.wait_for(
            producer.cleanup(),
            timeout=10.0
        )
        print("Cleanup completed")
    # Start tasks
    taskP = asyncio.create_task(run_producer(producer))
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

    try:
        await asyncio.wait_for(
            asyncio.gather(*all_tasks, return_exceptions=True),
            timeout=15.0
        )
        await shutdown()
    except asyncio.TimeoutError:
        print("Warning: Shutdown timed out")
        
    print("Shutdown complete.")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
