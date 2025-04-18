from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any

@dataclass
class Trade:
    exchange: str
    symbol: str
    price: float
    quantity: float
    timestamp: datetime
    is_buyer_maker: bool
    trade_id: str = None
    quote_quantity: float = None

    def get_table_name(self) -> str:
        return f"tbl_{self.exchange}_{self.symbol.replace('.', '_').lower()}"

    @classmethod
    def from_kafka_message(cls, message: Dict[str, Any]):
        return cls(
            symbol=message['key'].decode('utf-8').lower(),
            price=message['value']['price'],
            quantity=message['value']['quantity'],
            timestamp=datetime.fromtimestamp(message['value']['timestamp']/1000),
            is_buyer_maker=message['value']['is_buyer_maker'],
            trade_id=message['value'].get('trade_id'),
            quote_quantity=message['value'].get('quote_quantity')
        )

@dataclass
class RawTrade:
    symbol: str
    price: float
    quantity: float
    timestamp: datetime
    is_buyer_maker: bool
    trade_id: str = None
    quote_quantity: float = None

@dataclass
class AggregatedTrade:
    symbol: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    total_quantity: float
    total_trades: int
    buy_quantity: float
    sell_quantity: float
    start_time: datetime
    end_time: datetime

@dataclass
class CachedTradeData:
    symbol: str
    trades: list[Trade]
    rsi: float
    macd: float
    ma20: float

    def to_dict(self):
        return {
            'symbol': self.symbol,
            'trades': [trade.__dict__ for trade in self.trades],
            'rsi': self.rsi,
            'macd': self.macd,
            'ma20': self.ma20
        }