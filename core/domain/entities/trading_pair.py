from dataclasses import dataclass
from typing import Dict, List, Any
from core.domain.value_objects.volume_slots import VolumeSlots

@dataclass
class TradingPair:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    base_asset_precision: int
    quote_asset_precision: int
    is_spot_trading_allowed: bool
    is_margin_trading_allowed: bool
    filters: List[Dict[str, Any]]
    maker_commission: float
    taker_commission: float
    max_margin_allowed: float
    volume_slots: str
    trade_slots: str
    slots_stats: str
    total_volume: float = 0.0  # Added for historical data
    total_trades: int = 0      # Added for historical data
    volume_slots: VolumeSlots  # Value object
    
    @classmethod
    def from_binance_data(cls, binance_data: Dict[str, Any], 
                         maker_commission: float, 
                         taker_commission: float) -> 'TradingPair':
        return cls(
            symbol=binance_data['symbol'].lower(),
            status=binance_data['status'],
            base_asset=binance_data['baseAsset'],
            quote_asset=binance_data['quoteAsset'],
            base_asset_precision=binance_data['baseAssetPrecision'],
            quote_asset_precision=binance_data['quoteAssetPrecision'],
            is_spot_trading_allowed=binance_data['isSpotTradingAllowed'],
            is_margin_trading_allowed=binance_data['isMarginTradingAllowed'],
            filters=binance_data['filters'],
            maker_commission=maker_commission,
            taker_commission=taker_commission,
            max_margin_allowed=1000.0 if binance_data['isMarginTradingAllowed'] else 0.0,
            total_volume=0.0,
            total_trades=0, 
            volume_slots="{}", 
            trade_slots="{}",
            slots_stats="{}"
        )
    
    def is_tradable(self) -> bool:
        return self.is_spot_trading_allowed