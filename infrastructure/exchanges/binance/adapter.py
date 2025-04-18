import datetime
import json
import websockets
from core.domain.trade import RawTrade
from typing import AsyncIterator
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter

class BinanceWebSocketAdapter(IWebSocketAdapter):
    def __init__(self):
        self.websocket = None
        self.active_symbols = set()

    async def connect(self, symbols: list[str]):
        stream_names = [f"{s.lower()}@trade" for s in symbols]
        uri = f"wss://stream.binance.com:9443/stream?streams={'/'.join(stream_names)}"
        self.websocket = await websockets.connect(uri)
        self.active_symbols.update(symbols)

    async def disconnect(self):
        if self.websocket:
            await self.websocket.close()
            self.active_symbols.clear()

    async def stream_trades(self) -> AsyncIterator[RawTrade]:
        async for message in self.websocket:
            data = json.loads(message)
            if 'data' in data:
                trade_data = data['data']
                yield RawTrade(
                    symbol=trade_data['s'].lower(),
                    price=float(trade_data['p']),
                    quantity=float(trade_data['q']),
                    timestamp=datetime.fromtimestamp(trade_data['T']/1000),
                    is_buyer_maker=trade_data['m'],
                    trade_id=str(trade_data['t'])
                )