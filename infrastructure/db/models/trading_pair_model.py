from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any
from core.domain.entities.trading_pair import TradingPair

@dataclass
class TradingPairModel:
    id: int
    symbol: str
    base_asset: str
    quote_asset: str
    is_spot_trading_allowed: bool
    is_margin_trading_allowed: bool
    maker_commission: float
    taker_commission: float
    volume_slots_json: str
    created_at: datetime
    updated_at: datetime
    
    def to_entity(self) -> 'TradingPair':
        from core.domain.entities.trading_pair import TradingPair
        from core.domain.value_objects.volume_slots import VolumeSlots
        
        return TradingPair(
            symbol=self.symbol,
            base_asset=self.base_asset,
            quote_asset=self.quote_asset,
            is_spot_trading_allowed=self.is_spot_trading_allowed,
            is_margin_trading_allowed=self.is_margin_trading_allowed,
            maker_commission=self.maker_commission,
            taker_commission=self.taker_commission,
            volume_slots=VolumeSlots.from_json(self.volume_slots_json)
        )