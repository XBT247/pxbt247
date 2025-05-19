from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import asdict
from application.services.volume_points_calculator import VolumePointsCalculator
from core.domain.entities.trading_pair import TradingPair
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository

class CalculateVolumePointsUseCase:    
    DEFAULT_LOOKBACK_DAYS = 30
    def __init__(self, trading_pairs_repo: ITradingPairsRepository, trades_repo: ITradesRepository, calculator: Optional[VolumePointsCalculator] = None, logger: logging.Logger = None) -> None:
        self.trading_pairs_repo = trading_pairs_repo
        self.trades_repo = trades_repo
        self.calculator = calculator or VolumePointsCalculator(trading_pairs_repo, trades_repo, logger)
        self.logger = logger or logging.getLogger(__name__)

    async def execute(self, specific_symbols: Optional[List[str]] = None) -> Dict[str, dict]:
        """
        Calculate and store volume points for trading pairs
        Args:
            specific_symbols: Optional list of symbols to process (None processes all)
        Returns:
            Dictionary with results for each processed symbol
        """
        results = {}
        try:
            # Get all trading pairs to calculate points
            self.logger.info("UseCase: fetch_trading_pairs specific_symbols %s", specific_symbols)
            trading_pairs = await self.trading_pairs_repo.fetch_trading_pairs()                
            if specific_symbols:
                trading_pairs = [p for p in trading_pairs if p.symbol in specific_symbols]
            
            for pair in trading_pairs:
                try:
                    symbol = pair.symbol
                    #self.logger.info("UseCase: load_slots_from_db for symbol %s", symbol)
                    await self.calculator.load_slots_from_db(symbol, pair.volume_slots, pair.trade_slots, pair.slots_stats)
                    # Get historical data with validation
                    historical_data = await self.calculator._get_validated_historical_data(pair.symbol, self.DEFAULT_LOOKBACK_DAYS)
                    self.logger.info("UseCase: historical_data %s", historical_data)
                    if not historical_data:
                        continue
                        
                     # Calculate metrics
                    avg_volume, avg_trades = self._calculate_averages(historical_data)
                    self.logger.info(
                        f"UseCase: Calculated averages for {pair.symbol}: "
                        f"Avg Volume={avg_volume:.2f}, Avg Trades={avg_trades:.2f}"
                    )
                    # Calculate points
                    result = await self.calculator.calculate_points(
                        symbol=pair.symbol,
                        total_volume=avg_volume,
                        total_trades=avg_trades
                    )
                    if result is not None:
                        print(f"Result: {result}")
                    if result:
                        volume_points, trades_points, iceberg_score = result
                        results[symbol] = {
                            'volume_points': volume_points,
                            'trades_points': trades_points,
                            'iceberg_score': iceberg_score,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        
                        self.logger.info(
                            f"Calculated points for {pair.symbol}: "
                            f"Volume={volume_points:.2f}, "
                            f"Trades={trades_points:.2f}, "
                            f"Iceberg={iceberg_score:.2f}"
                        )
                        # Update slots data if needed
                        await self._update_slot_data(pair.symbol)
                except Exception as e:
                    self.logger.error(f"Failed to calculate points for {pair.symbol}: {e}")
                    results[symbol] = {'error': str(e)}

            return results
        
        except Exception as e:
            self.logger.error(f"Error in CalculateVolumePointsUseCase: {e}")
            raise

    def _calculate_averages(self, historical_data: List[dict]) -> Tuple[float, float]:
        """Calculate average volume and trades with validation"""
        valid_volumes = [d.volume for d in historical_data if d.volume > 0]
        valid_trades = [d.trades for d in historical_data if d.trades > 0]
        
        avg_volume = sum(valid_volumes) / len(valid_volumes) if valid_volumes else 0
        avg_trades = sum(valid_trades) / len(valid_trades) if valid_trades else 0
        
        return avg_volume, avg_trades

    async def _update_slot_data(self, symbol: str) -> None:
        """Update slot data in repository if slots exist for this symbol"""
        if symbol in self.calculator.volume_slots and symbol in self.calculator.trades_slots:
            await self.trading_pairs_repo.update_slots_data(
                symbol=symbol,
                volume_slots=json.dumps([asdict(s) for s in self.calculator.volume_slots[symbol]]),
                trade_slots=json.dumps([asdict(s) for s in self.calculator.trades_slots[symbol]]),
                stats=json.dumps({
                    'median_vpt': self.calculator.median_vpt.get(symbol, 0),
                    'vpt_std_dev': self.calculator.vpt_std_dev.get(symbol, 0),
                    'last_updated': datetime.utcnow().isoformat()
                })
            )
    