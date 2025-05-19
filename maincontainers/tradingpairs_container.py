import asyncio
from logging.config import dictConfig
from typing import Optional
from datetime import datetime, timedelta
from core.domain.exchange import ExchangeConfig
from core.exchange_factory import ExchangeFactory
from core.interfaces.exchanges.ibinance_client import IBinanceClient
from core.interfaces.iproducer import ITradeProducer
from core.use_cases.uc_fetch_trading_pairs import FetchTradingPairsUseCase
from core.use_cases.uc_calculate_volume_points import CalculateVolumePointsUseCase
from infrastructure.db.dbhandler import DBHandler
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from infrastructure.exchanges.binance.binance_client import BinanceClient
from infrastructure.db.repositories.tradingpairs_repository import TradingPairsRepository
from infrastructure.exchanges.binance.factory import BinanceExchangeFactory
#from infrastructure.messaging.kafka_producer import KafkaProducer
from infrastructure.config.settings import load_config
from core.logging import Logger

class TradingPairsContainer:
    def __init__(self, exchange: str = "binance"):
        self.exchange = exchange
        config_data = load_config(exchange)
        self.config = ExchangeConfig(exchange, config_data)
        self.logger = Logger.get_logger(f"container.{exchange}")
        self.binance_client: Optional[IBinanceClient] = None
        self.trading_pairs_repo: Optional[ITradingPairsRepository] = None
        #self.message_producer: Optional[ITradeProducer] = None
        self.fetch_use_case: Optional[FetchTradingPairsUseCase] = None
        self.points_use_case: Optional[CalculateVolumePointsUseCase] = None
        self._running = False

    async def initialize(self):
        """Initialize all dependencies"""
        try:
            # Initialize Binance client
            self.binance_client = BinanceClient(
                api_key= '', #self.config['binance']['api_key'],
                secret_key= '', #self.config['binance']['secret_key'],
                base_url=self.config.get_url('base_url')
            )
           
            self.trading_pairs_repo = ExchangeFactory.create_repository_tradingpairs(self.exchange, self.config, logger=self.logger)
            
            # Initialize message producer
            #self.message_producer = KafkaProducer(self.config['kafka']['bootstrap_servers'])
            
            # Initialize use cases
            self.fetch_use_case = FetchTradingPairsUseCase(
                binance_client=self.binance_client,
                trading_pairs_repo=self.trading_pairs_repo
            )
            
            self.points_use_case = CalculateVolumePointsUseCase(
                trading_pairs_repo=self.trading_pairs_repo, 
                trades_repo=ExchangeFactory.create_repository_trades(self.exchange, self.config, logger=self.logger)
            )
            
            self.logger.info("TradingPairs container initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize TradingPairs container: {e}")
            await self.shutdown()
            raise

    async def run_hourly_task(self):
        """Execute both fetch and points calculation at xx:00:00"""
        self.logger.info("Starting hourly task")
        while self._running:
            await asyncio.sleep(1)  # Prevent busy waiting
            try:
                now = datetime.utcnow()
                next_hour = (now + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0
                )
                wait_seconds = (next_hour - now).total_seconds()
                
                self.logger.info(f"Next execution at {next_hour} (in {wait_seconds:.1f} seconds)")
                await asyncio.sleep(wait_seconds)
                
                # Execute both operations
                self.logger.info("Starting hourly trading pairs update")
                trading_pairs = await self.fetch_use_case.execute()
                self.logger.info(f"Fetched {len(trading_pairs)} trading pairs")
                
                self.logger.info("Starting volume points calculation")
                await self.points_use_case.execute(['btcusdt', 'ethusdt', 'bnbusdt'])
                await self.points_use_case.calculator.run_hourly_update()
                self.logger.info("Completed volume points calculation")
                
            except asyncio.CancelledError:
                self.logger.info("Hourly task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in hourly task: {e}")
                await asyncio.sleep(60)  # Wait before retry

    async def run(self):
        """Start the container's main workflow"""
        if not all([self.fetch_use_case, self.points_use_case]):
            raise RuntimeError("Container not initialized")
            
        self._running = True
    
        # Create task for hourly execution
        hourly_task = asyncio.create_task(self.run_hourly_task())
        
        # Run immediately on startup
        self.logger.info("TPC_Container Running initial fetch and points calculation")
        trading_pairs = await self.fetch_use_case.execute()
        self.logger.info(f"TPC_Container Fetched {len(trading_pairs)} trading pairs")
        await self.points_use_case.execute(['btcusdt', 'ethusdt', 'bnbusdt'])
        await self.points_use_case.calculator.run_hourly_update()

        
        # Start hourly scheduled task
        #await self.run_hourly_task()

    async def shutdown(self):
        """Clean up resources"""
        self._running = False
        shutdown_tasks = []
        
        if hasattr(self, 'message_producer') and self.message_producer:
            shutdown_tasks.append(self.message_producer.stop())
            
        if hasattr(self, 'trading_pairs_repo') and self.trading_pairs_repo:
            if hasattr(self.trading_pairs_repo, 'close'):
                shutdown_tasks.append(self.trading_pairs_repo.close())
                
        try:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")