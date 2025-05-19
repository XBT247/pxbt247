from dataclasses import dataclass
from datetime import datetime

@dataclass
class Trade:
    symbol: str
    price: float
    quantity: float
    timestamp: datetime
    is_buyer_maker: bool