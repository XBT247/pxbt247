import logging
from typing import List
from core.domain.entities.trading_pair import TradingPair
from core.interfaces.iproducer import ITradeProducer
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository
from core.interfaces.exchanges.ibinance_client import IBinanceClient

logger = logging.getLogger(__name__)

class FetchTradingPairsUseCase:
    def __init__(
        self,
        binance_client: IBinanceClient,
        trading_pairs_repo: ITradingPairsRepository,
        #message_producer: ITradeProducer
    ):
        self.binance_client = binance_client
        self.trading_pairs_repo = trading_pairs_repo
        #self.message_producer = message_producer

    async def execute(self) -> List[TradingPair]:
        """Fetch, process and store trading pairs"""
        try:
            # Get account info for commissions
            account_info = await self.binance_client.get_account_info()
            maker_commission = account_info.get('makerCommission', 10) / 10000
            taker_commission = account_info.get('takerCommission', 15) / 10000

            # Get trading pairs from Binance
            exchange_info = await self.binance_client.get_exchange_info()
            trading_pairs = []

            for symbol_info in exchange_info['symbols']:
                if symbol_info['status'] == 'TRADING':
                    trading_pair = TradingPair.from_binance_data(
                        symbol_info,
                        maker_commission,
                        taker_commission
                    )
                    trading_pairs.append(trading_pair)

                    # Save to repository
                    await self.trading_pairs_repo.save_trading_pair(trading_pair)

                    # Publish to message bus
                    #await self.message_producer.publish_trading_pair(trading_pair)

            logger.info(f"Processed {len(trading_pairs)} trading pairs")
            return trading_pairs

        except Exception as e:
            logger.error(f"Error in FetchTradingPairsUseCase: {e}")
            raise