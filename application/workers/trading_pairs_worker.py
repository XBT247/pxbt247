import asyncio
import logging
from core.use_cases.uc_fetch_trading_pairs import FetchTradingPairsUseCase
from infrastructure.db.dbhandler import DBHandler
from infrastructure.exchanges.binance.binance_client import BinanceClient
from infrastructure.db.repositories.tradingpairs_repository import TradingPairsRepository
from infrastructure.messaging.kafka_producer import KafkaProducer

logger = logging.getLogger(__name__)

class TradingPairsWorker:
    def __init__(self, config: dict):
        self.config = config
        self.running = False

    async def start(self):
        self.running = True
        
        # Initialize dependencies
        binance_client = BinanceClient(
            self.config['binance']['api_key'],
            self.config['binance']['secret_key'],
            self.config['binance']['base_url']
        )
        
        trading_pairs_repo = TradingPairsRepository(DBHandler(self.config['database']))
        
        kafka_producer = KafkaProducer(
            self.config['kafka']['bootstrap_servers']
        )
        
        use_case = FetchTradingPairsUseCase(
            binance_client,
            trading_pairs_repo,
            kafka_producer
        )

        while self.running:
            try:
                await use_case.execute()
                await asyncio.sleep(3600)  # Run hourly
            except asyncio.CancelledError:
                logger.info("Trading pairs worker stopped")
                break
            except Exception as e:
                logger.error(f"Error in trading pairs worker: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def stop(self):
        self.running = False