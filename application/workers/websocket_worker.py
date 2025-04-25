# websocket_worker.py
import asyncio
import logging
from typing import List, Dict, AsyncIterator
from datetime import datetime
from collections import defaultdict
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from core.domain.trade import RawTrade

class WebSocketWorker:
    def __init__(
        self,
        adapter: IWebSocketAdapter,
        symbols: List[str],
        max_reconnect_attempts: int = 5,
        health_check_interval: int = 60
    ):
        self.adapter = adapter
        self.symbols = symbols
        self.max_attempts = max_reconnect_attempts
        self.health_check_interval = health_check_interval
        self.connection_attempts = defaultdict(int)
        self.active_connections = {}
        self._shutdown_flag = False
        self.logger = logging.getLogger(__name__)

    async def run(self) -> AsyncIterator[RawTrade]:
        """Main worker loop with connection monitoring and graceful reconnects"""
        connection_id = ",".join(sorted(self.symbols))
        
        # Start health monitoring task
        monitor_task = asyncio.create_task(self._connection_monitor())
        
        try:
            while not self._shutdown_flag:
                try:
                    self.connection_attempts[connection_id] += 1
                    
                    if self.connection_attempts[connection_id] > self.max_attempts:
                        await self._handle_max_attempts(connection_id)
                        continue
                        
                    await self._establish_connection(connection_id)
                    
                    async for trade in self.adapter.stream_trades():
                        self._update_connection_activity(connection_id)
                        yield trade
                        
                except Exception as e:
                    await self._handle_connection_error(connection_id, e)
                    
        finally:
            monitor_task.cancel()
            await self._cleanup_resources()

    async def _establish_connection(self, connection_id: str):
        """Establish new WebSocket connection"""
        self.active_connections[connection_id] = {
            'status': 'connecting',
            'last_activity': datetime.now(),
            'symbols': self.symbols
        }
        
        await self.adapter.connect(self.symbols)
        self.active_connections[connection_id]['status'] = 'connected'
        self.connection_attempts[connection_id] = 0
        self.logger.info(f"WebSocket connected for symbols: {self.symbols}")

    async def _connection_monitor(self):
        """Periodically check connection health and log status"""
        while not self._shutdown_flag:
            await asyncio.sleep(self.health_check_interval)
            self._log_connection_status()
            self._check_stale_connections()

    def _log_connection_status(self):
        """Log current connection status"""
        active = sum(1 for c in self.active_connections.values() 
                    if c['status'] == 'connected')
        total = len(self.active_connections)
        self.logger.info(
            f"WebSocket Status - Active: {active}/{total} connections | "
            f"Symbols: {[c['symbols'] for c in self.active_connections.values()]}"
        )

    def _check_stale_connections(self):
        """Detect and handle stale connections"""
        stale_timeout = self.health_check_interval * 2
        now = datetime.now()
        
        for conn_id, conn in self.active_connections.items():
            if (conn['status'] == 'connected' and 
                (now - conn['last_activity']).total_seconds() > stale_timeout):
                self.logger.warning(f"Stale connection detected: {conn_id}")
                conn['status'] = 'stale'

    def _update_connection_activity(self, connection_id: str):
        """Update last activity timestamp"""
        if connection_id in self.active_connections:
            self.active_connections[connection_id]['last_activity'] = datetime.now()

    async def _handle_max_attempts(self, connection_id: str):
        """Handle case when max reconnect attempts reached"""
        wait_time = 60  # Longer wait after max attempts
        self.logger.warning(
            f"Max connection attempts reached for {connection_id}. "
            f"Waiting {wait_time} seconds..."
        )
        self.active_connections[connection_id]['status'] = 'retrying'
        await asyncio.sleep(wait_time)
        self.connection_attempts[connection_id] = 0

    async def _handle_connection_error(self, connection_id: str, error: Exception):
        """Handle connection errors with exponential backoff"""
        if connection_id in self.active_connections:
            self.active_connections[connection_id]['status'] = 'error'
            
        wait_time = min(2 ** self.connection_attempts[connection_id], 30)
        self.logger.error(
            f"WebSocket error for {connection_id} (attempt {self.connection_attempts[connection_id]}): "
            f"{str(error)}. Retrying in {wait_time} seconds..."
        )
        await asyncio.sleep(wait_time)

    async def _cleanup_resources(self):
        """Clean up all resources during shutdown"""
        self.logger.info("Shutting down WebSocket worker...")
        try:
            await self.adapter.disconnect()
        except Exception as e:
            self.logger.error(f"Error during WebSocket disconnect: {str(e)}")
        finally:
            self.active_connections.clear()

    async def shutdown(self):
        """Initiate graceful shutdown"""
        self._shutdown_flag = True
        await self._cleanup_resources()