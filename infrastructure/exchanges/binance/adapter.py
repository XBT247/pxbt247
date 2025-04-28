# infrastructure/exchanges/binance/adapter.py
import websockets
import json
from typing import AsyncIterator, List
from urllib.parse import quote
from core import logging
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter

class BinanceWebSocketAdapter(IWebSocketAdapter):
    MAX_STREAMS_PER_CONNECTION = 50  # Binance's documented limit
    ws_base_url = "wss://stream.binance.com:9443/stream"
    
    def __init__(self, ws_base_url: str = "wss://stream.binance.com:9443", logger: logging.Logger = None):
        self.ws_base_url = ws_base_url
        self.connections: dict = {}  # Track multiple connections
        self.logger = logger

    async def connect(self, symbols: List[str], connection_id: str) -> None:
        """Establish connection with proper URI formatting"""
        if len(symbols) > self.MAX_STREAMS_PER_CONNECTION:
            raise ValueError(f"Max {self.MAX_STREAMS_PER_CONNECTION} streams per connection")
            
        streams = [f"{s.lower()}@trade" for s in symbols]
        encoded_streams = quote('/'.join(streams))
        uri = f"{self.ws_base_url}?streams={encoded_streams}"
        
        self.logger.debug(f"Connecting to {uri[:100]}...")  # Log truncated URI
        self.connections[connection_id] = {
            'websocket': await websockets.connect(uri, ping_interval=20, ping_timeout=10),
            'symbols': symbols
        }

    async def stream_trades(self, connection_id: str) -> AsyncIterator[dict]:
        """Stream trades with proper error handling"""
        if connection_id not in self.connections:
            raise ValueError(f"Unknown connection: {connection_id}")
            
        try:
            async for message in self.connections[connection_id]['websocket']:
                data = json.loads(message)
                if 'data' in data:
                    yield data['data']
        except websockets.exceptions.ConnectionClosed as e:
            self.logger.error(f"Connection {connection_id} closed: {e.code} - {e.reason}")
            raise
        except Exception as e:
            self.logger.error(f"Stream error {connection_id}: {str(e)}")
            raise

    async def shutdown(self, connection_id: str = None) -> None:
        """Graceful shutdown with error handling"""
        if connection_id:
            connection = self.connections.pop(connection_id, None)  # Use pop to avoid KeyError
            if connection:
                try:
                    await connection['websocket'].close()
                except Exception as e:
                    self.logger.error(f"Error closing {connection_id}: {str(e)}")
            else:
                self.logger.warning(f"Connection ID {connection_id} not found during shutdown")
        else:
            for conn_id in list(self.connections.keys()):
                await self.shutdown(conn_id)