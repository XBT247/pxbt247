import asyncio

from maincontainers.container import Container
#from composition_root.container import Container

async def main():
    container = Container("binance")
    await container.initialize()
    
    try:
        await container.producer_service.run()
    finally:
        await container.shutdown()

if __name__ == "__main__":
    asyncio.run(main())