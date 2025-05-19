from dataclasses import dataclass
from typing import Optional

@dataclass
class TradeDBModel:
    id: int
    server_time: int
    open: float
    close: float
    avg_price: Optional[float]
    high: float
    low: float
    high200: Optional[float]
    low200: Optional[float]
    qusd_price: Optional[float]
    volume: int
    trades: int
    total_volume_buy: int
    total_volume_sell: int
    actual_vol_sell: Optional[int]
    actual_vol_buy: Optional[int]
    actual_volume: Optional[int]
    total_trades_buy: int
    total_trades_sell: int
    buy_percentage: Optional[int]
    volume_points: Optional[float]
    trades_points: Optional[float]
    sum_volume_points_100: Optional[float]
    sum_trade_points_100: Optional[float]
    qtr_history_30: Optional[float]
    qtr_history_100: Optional[float]
    qtr_avg_history_100: Optional[float]
    order_side: Optional[int]
    order_type: Optional[int]
    order_price: Optional[float]
    order_text: Optional[str]
    is_sidelined: Optional[int]
    dir: Optional[int]
