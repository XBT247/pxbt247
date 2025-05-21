import numpy as np
import json
from typing import Dict, Tuple, List, Optional
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from infrastructure.db.interfaces.itrade_repository import ITradesRepository
from infrastructure.db.interfaces.itradingpairs_repository import ITradingPairsRepository

@dataclass
class VolumeSlot:
    min_value: float
    max_value: float
    weight: float
    count: int = 0
    percentile_min: float = 0.0
    percentile_max: float = 0.0

class VolumePointsCalculator:
    DEFAULT_SLOT_COUNT = 11
    DEFAULT_MIN_PERCENTILE = 2.0
    DEFAULT_MAX_PERCENTILE = 90.0
    DEFAULT_MIN_WEIGHT = 0.5
    DEFAULT_MAX_WEIGHT = 11.0
    DEFAULT_ICEBERG_Z_SCORE_THRESHOLD = 3.0
    DEFAULT_LOOKBACK_DAYS = 30
    MIN_DATA_POINTS_REQUIRED = 10  # Minimum data points for meaningful calculation

    def __init__(self, trading_pairs_repo: ITradingPairsRepository, trades_repo: ITradesRepository, logger: logging.Logger = None):
        self.repo = trading_pairs_repo
        self.trades_repo = trades_repo
        self.volume_slots: Dict[str, List[VolumeSlot]] = {}
        self.trades_slots: Dict[str, List[VolumeSlot]] = {}
        self.median_vpt: Dict[str, float] = {}
        self.vpt_std_dev: Dict[str, float] = {}
        self.slot_config: Dict[str, Dict[str, float]] = {}  # Per-symbol configuration
        self.logger = logger or logging.getLogger(__name__)

    async def initialize_all_symbols(self) -> None:
        """
        Initialize slots for all trading pairs
        """
        try:
            trading_pairs = await self.repo.fetch_trading_pairs()            
            for pair in trading_pairs:
                symbol = pair.symbol
                #logger.info("UseCase: load_slots_from_db for symbol %s", symbol)
                await self.load_slots_from_db(symbol, pair.volume_slots, pair.trade_slots, pair.slots_stats)
        except Exception as e:
            self.logger.error(f"Failed to initialize all symbols: {e}")

    async def run_hourly_update(self) -> None:
        """
        Main method to run hourly updates for all symbols
        """
        try:
            start_time = datetime.now(timezone.utc)  # Fixed timezone usage
            self.logger.info("Starting hourly volume points update")
            
            trading_pairs = await self.repo.fetch_trading_pairs()
            self.logger.info(f"Fetched {len(trading_pairs)} trading pairs for hourly update")
            processed_count = 0
            
            for pair in trading_pairs:
                symbol = pair.symbol
                try:
                    self.logger.info(f"Processing symbol {symbol}")
                    await self.process_symbol(symbol)
                    processed_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to process symbol {symbol}: {e}")
                    continue
            
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()  # Fixed timezone usage
            self.logger.info(f"Hourly update completed. Processed {processed_count}/{len(trading_pairs)} symbols in {duration:.2f} seconds")
            
        except Exception as e:
            self.logger.error(f"Hourly update failed: {e}")

    async def process_symbol(self, symbol: str, lookback_days: int = 30) -> None:
        """
        Process a single symbol: calculate slots and save to database
        """
        historical_data = await self._get_validated_historical_data(symbol, lookback_days)
        
        if not historical_data:
            self.logger.warning(f"No historical data found for {symbol}")
            return
        self.logger.info(f"Processing symbol {symbol} with {len(historical_data)} data points")
        config = self.slot_config.get(symbol, {})
        #self.logger.info(f"Slot config for {symbol}: {config}")
        # Process volumes
        total_volumes = [d.volume for d in historical_data if d.volume > 0]        
        self.volume_slots[symbol] = self._create_dynamic_slots(
            values=total_volumes,
            slot_count=config.get('slot_count', self.DEFAULT_SLOT_COUNT),
            min_weight=config.get('min_weight', self.DEFAULT_MIN_WEIGHT),
            max_weight=config.get('max_weight', self.DEFAULT_MAX_WEIGHT),
            min_percentile=config.get('min_percentile', self.DEFAULT_MIN_PERCENTILE),
            max_percentile=config.get('max_percentile', self.DEFAULT_MAX_PERCENTILE)
        )
        #self.logger.info(f"Volume slots for {symbol}: {self.volume_slots[symbol]}")
        
        # Process trades
        total_trades = [d.trades for d in historical_data if d.trades > 0]
        self.trades_slots[symbol] = self._create_dynamic_slots(
            values=total_trades,
            slot_count=config.get('slot_count', self.DEFAULT_SLOT_COUNT),
            min_weight=config.get('min_weight', self.DEFAULT_MIN_WEIGHT),
            max_weight=config.get('max_weight', self.DEFAULT_MAX_WEIGHT),
            min_percentile=config.get('min_percentile', self.DEFAULT_MIN_PERCENTILE),
            max_percentile=config.get('max_percentile', self.DEFAULT_MAX_PERCENTILE)
        )
        
        # Calculate volume per trade statistics
        vpt_values = [v/t for v, t in zip(total_volumes, total_trades) if t > 0]
        self.median_vpt[symbol] = float(np.median(vpt_values)) if vpt_values else 0
        self.vpt_std_dev[symbol] = float(np.std(vpt_values)) if vpt_values else 0
        
        # Save to database
        await self._save_slots_to_db(symbol)

    async def _get_validated_historical_data(self, symbol: str, lookback_days: int = 30) -> Optional[List[dict]]:
        """Fetch and validate historical data"""
        historical_data = await self.trades_repo.fetch_trades_history(symbol)
        
        if not historical_data:
            self.logger.warning(f"No historical data found for {symbol}")
            return None
            
        if len(historical_data) < self.MIN_DATA_POINTS_REQUIRED:
            self.logger.warning(
                f"Insufficient data points ({len(historical_data)}) for {symbol}. "
                f"Minimum {self.MIN_DATA_POINTS_REQUIRED} required."
            )
            return None
            
        return historical_data
    
    def _create_dynamic_slots(self, values: List[float], slot_count: int, 
                            min_weight: float, max_weight: float,
                            min_percentile: float = 2, max_percentile: float = 90) -> List[VolumeSlot]:
        """
        Create dynamic weighted slots between specified percentiles
        """
        if not values or slot_count < 2:
            return []
            
        values = np.array(values)
        values = values[values > 0]  # Filter out zeros
        
        if len(values) < 10:
            return []
        
        # Calculate base percentiles (2% to 90%)
        base_percentiles = np.linspace(min_percentile, max_percentile, num=slot_count)
        base_edges = np.percentile(values, base_percentiles)
        
        # Add absolute min/max to handle tails
        min_value = np.min(values)
        max_value = np.max(values)
        
        # Create all edges including tails
        all_edges = np.concatenate([[min_value], base_edges, [max_value]])
        
        # Create slots
        slots = []
        weight_step = (max_weight - min_weight) / (slot_count - 1)
        
        # First slot (0% to 2%)
        slots.append(VolumeSlot(
            min_value= round(float(min_value), 2),
            max_value=round(float(base_edges[0]), 2),
            weight=round(min_weight, 2),
            count=int(np.sum(values <= base_edges[0])),
            percentile_min=0.0,
            percentile_max=round(min_percentile, 2)
        ))
        
        # Middle slots (2% to 90%)
        for i in range(len(base_edges) - 1):
            if i == 0:
                min_weight = 1
            weight = min_weight + (i * weight_step)
            percentile_min = min_percentile + (i * (max_percentile - min_percentile) / (slot_count - 1))
            percentile_max = percentile_min + (max_percentile - min_percentile) / (slot_count - 1)
            
            slots.append(VolumeSlot(
                min_value=round(float(base_edges[i]), 2),
                max_value=round(float(base_edges[i+1]), 2),
                weight=round(float(weight), 2),
                count=int(np.sum((values > base_edges[i]) & (values <= base_edges[i+1]))),
                percentile_min=round(percentile_min, 2),
                percentile_max=round(percentile_max, 2)
            ))
        
        # Last slot (90% to 100%)
        slots.append(VolumeSlot(
            min_value=round(float(base_edges[-1]), 2),
            max_value=round(float(max_value), 2),
            weight=round(max_weight, 2),
            count=int(np.sum(values > base_edges[-1])),
            percentile_min=round(max_percentile, 2),
            percentile_max=round(100.0, 2)
        ))
        
        # Handle case where we might have duplicate edges
        unique_slots = []
        seen_edges = set()
        
        for slot in slots:
            edge_key = (slot.min_value, slot.max_value)
            if edge_key not in seen_edges:
                unique_slots.append(slot)
                seen_edges.add(edge_key)
        
        return unique_slots

    async def _save_slots_to_db(self, symbol: str) -> None:
        """
        Save calculated slots to the database
        """
        try:
            volume_slots_json = json.dumps(
                [asdict(slot) for slot in self.volume_slots.get(symbol, [])],
                default=lambda o: int(o) if isinstance(o, np.integer) else float(o) if isinstance(o, np.floating) else o
            )
            trades_slots_json = json.dumps(
                [asdict(slot) for slot in self.trades_slots.get(symbol, [])],
                default=lambda o: int(o) if isinstance(o, np.integer) else float(o) if isinstance(o, np.floating) else o
            )
            stats = {
                'median_vpt': self.median_vpt.get(symbol, 0),
                'vpt_std_dev': self.vpt_std_dev.get(symbol, 0),
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            stats_json = json.dumps(stats)
            
            await self.repo.update_slots_data(
                symbol=symbol,
                volume_slots=volume_slots_json or '[]',
                trade_slots=trades_slots_json or '[]',
                stats=stats_json or '{}'
            )
            
            self.logger.debug(f"Saved slots data for {symbol}")
        except Exception as e:
            self.logger.error(f"Failed to save slots data for {symbol}: {e}")

    async def calculate_points(self, symbol: str, total_volume: float, 
                             total_trades: int) -> Optional[Tuple[float, float, float]]:
        """
        Calculate points using stored slot data
        """
        try:                           
            if not self.volume_slots.get(symbol) or not self.trades_slots.get(symbol):
                return None
            
            volume_points = self._get_weighted_value(total_volume, self.volume_slots[symbol])
            trades_points = self._get_weighted_value(total_trades, self.trades_slots[symbol])
            iceberg_score = self._detect_iceberg(total_volume, total_trades, symbol)
            
            return volume_points, trades_points, iceberg_score
        except Exception as e:
            self.logger.error(f"Point calculation failed for {symbol}: {e}")
            return None

    async def load_slots_from_db(self, symbol: str, volume_slots:str, trade_slots:str, slots_stats:str) -> bool:
        """
        Load slots data from database
        """
        try:
            if not volume_slots or not trade_slots or not slots_stats:
                #self.logger.warning(f"Slots data for {symbol} is None")
                return False
            self.logger.info(f"{symbol} Volume slots: {volume_slots}, Trade slots: {trade_slots}, Stats: {slots_stats}")
            self.volume_slots[symbol] = [VolumeSlot(**slot) for slot in json.loads(volume_slots)]
            self.trades_slots[symbol] = [VolumeSlot(**slot) for slot in json.loads(trade_slots)]
            
            stats = json.loads(slots_stats)
            self.median_vpt[symbol] = stats['median_vpt']
            self.vpt_std_dev[symbol] = stats['vpt_std_dev']
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to load slots data for {symbol}: {e}")
            return False

    def _get_weighted_value(self, value: float, slots: List[VolumeSlot]) -> float:
        """Get weight for value based on slots"""
        if not slots:
            return 0.5
            
        for slot in slots:
            if slot.min_value <= value <= slot.max_value:
                return slot.weight
            elif slot.max_value == float('inf') and value > slot.min_value:
                return slot.weight
            elif slot.min_value == -float('inf') and value < slot.max_value:
                return slot.weight
                
        return slots[-1].weight  # Fallback to max weight

    def _detect_iceberg(self, volume: float, trades: int, symbol: str) -> float:
        """Calculate iceberg score"""
        if trades == 0 or symbol not in self.median_vpt or symbol not in self.vpt_std_dev:
            return 0
            
        volume_per_trade = volume / trades
        median = self.median_vpt[symbol]
        std_dev = self.vpt_std_dev[symbol]
        
        if std_dev == 0:
            return 0
            
        z_score = (volume_per_trade - median) / std_dev
        return min(max(1 / (1 + np.exp(-(z_score - 3))), 0), 1)