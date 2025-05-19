import asyncio
from maincontainers.producer_container import ProducerContainer

async def main():
    container = ProducerContainer("binance")
    await container.initialize()
    
    try:
        await container.producer_service.run() 
    finally:
        await container.shutdown()
        if hasattr(container.producer_service, 'producer') and container.producer_service.producer:
            await container.producer_service.producer.stop()  # Ensure Kafka producer is stopped

if __name__ == "__main__":
    asyncio.run(main())