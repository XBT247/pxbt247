# application/workers/websocket_worker.py
import asyncio
from typing import AsyncIterator, List
from datetime import datetime, timedelta
from core import logging
from core.interfaces.iwebsocket_adapter import IWebSocketAdapter
from core.domain.trade import RawTrade

class WebSocketWorker:
    def __init__(
        self,
        adapter: IWebSocketAdapter,
        symbols: List[str],
        max_streams_per_connection: int = 50,  # Conservative default
        max_reconnect_attempts: int = 5,
        health_check_interval: int = 30,
        logger: logging.Logger = None
    ):
        self.logger = logger or logging.getLogger(__name__)  # Assign logger first
        self.adapter = adapter
        self.symbols = self._validate_symbols(symbols)  # Now logger is initialized
        self.max_streams = min(max_streams_per_connection, adapter.MAX_STREAMS_PER_CONNECTION)
        self.max_attempts = max_reconnect_attempts
        self.health_check_interval = health_check_interval
        self._shutdown_flag = False
        self._monitor_task = None
        self.connection_states = {}

    def _validate_symbols(self, symbols: List[str]) -> List[str]:
        """Filter out invalid symbols"""
        valid = []
        for s in symbols:
            # Validate symbols in the 'basequote' format (e.g., 'BTCUSDT')
            if isinstance(s, str) and len(s) > 3 and s.isalnum():
                valid.append(s)
            else:
                self.logger.info(f"Invalid symbol format: {s}")
        return valid

    async def run(self) -> AsyncIterator[RawTrade]:
        """Main execution with proper batching and error handling"""
        symbol_batches = self._create_batches()
        self._monitor_task = asyncio.create_task(self._monitor_connections())
        
        try:
            tasks = []
            for batch in symbol_batches:
                conn_id = f"batch_{len(tasks)}"
                tasks.append(self._manage_batch_connection(conn_id, batch))
            
            async for trade in self._merge_streams(tasks):
                yield trade
                
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    def _create_batches(self) -> List[List[str]]:
        """Create optimized batches respecting exchange limits"""
        batches = []
        current_batch = []
        
        for symbol in self.symbols:
            if len(current_batch) >= self.max_streams:
                batches.append(current_batch)
                current_batch = []
            current_batch.append(symbol)
        
        if current_batch:
            batches.append(current_batch)
            
        self.logger.info(f"Created {len(batches)} batches with max {self.max_streams} streams each")
        return batches

    async def _manage_batch_connection(self, conn_id: str, symbols: List[str]) -> AsyncIterator[RawTrade]:
        """Handle a single batch connection with retries"""
        attempts = 0
        
        while not self._shutdown_flag and attempts < self.max_attempts:
            attempts += 1
            self.connection_states[conn_id] = {
                'status': 'connecting',
                'attempt': attempts,
                'last_active': None
            }
            
            try:
                await self.adapter.connect(symbols, conn_id)
                self.connection_states[conn_id]['status'] = 'connected'
                
                async for trade_data in self.adapter.stream_trades(conn_id):
                    self.connection_states[conn_id]['last_active'] = datetime.now()
                    yield self._transform_trade(trade_data)
                    
            except Exception as e:
                self.logger.error(f"Error in _manage_batch_connection for {conn_id}: {e}")
                await self._handle_connection_error(conn_id, e, attempts)
                if attempts < self.max_attempts:
                    await asyncio.sleep(min(2 ** attempts, 30))  # Exponential backoff
            finally:
                await self.adapter.shutdown(conn_id)
                if conn_id in self.connection_states:
                    self.connection_states[conn_id]['status'] = 'disconnected'

    async def _handle_connection_error(self, conn_id: str, error: Exception, attempt: int):
        """Log and handle connection errors"""
        error_msg = str(error)
        if "404" in error_msg:
            self.logger.error(f"Invalid stream combination (404) in {conn_id}")
            if conn_id in self.connection_states:
                self.connection_states[conn_id]['status'] = 'invalid'
        elif "414" in error_msg:
            new_max = max(1, self.max_streams // 2)
            self.logger.warning(f"URI too long, reducing batch size to {new_max}")
            self.max_streams = new_max
        else:
            self.logger.error(f"Connection error {conn_id} (attempt {attempt}): {error_msg}")
        if conn_id in self.connection_states:
            self.connection_states[conn_id]['status'] = 'error'

    async def _monitor_connections(self):
        """Monitor and log connection states"""
        while not self._shutdown_flag:
            await asyncio.sleep(self.health_check_interval)
            active = sum(1 for s in self.connection_states.values() if s['status'] == 'connected')
            total = len(self.connection_states)
            self.logger.info(
                f"Connections: {active}/{total} active | "
                f"Recent errors: {sum(1 for s in self.connection_states.values() if s['status'] == 'error')}"
            )

    async def _merge_streams(self, tasks: List) -> AsyncIterator[RawTrade]:
        """Merge multiple streams with backpressure control"""
        queue = asyncio.Queue(maxsize=1000)
        async def _feed_queue(task):
            try:
                async for item in task:
                    await queue.put(item)
            except Exception as e:
                self.logger.error(f"Stream feeder error: {str(e)}")
        
        feeders = [asyncio.create_task(_feed_queue(task)) for task in tasks]
        
        try:
            while not self._shutdown_flag:
                yield await queue.get()
        finally:
            for task in feeders:
                task.cancel()

    def _transform_trade(self, data: dict) -> RawTrade:
        """Convert to domain model with validation"""
        try:
            #self.logger.info(f"{data['s']} - {data['p']} - {data['q']}")
            return RawTrade(
                symbol=data['s'].lower(),
                price=float(data['p']),
                quantity=float(data['q']),
                timestamp=datetime.fromtimestamp(data['T']/1000),
                is_buyer_maker=data['m'],
                trade_id=str(data['t'])
            )
        except (KeyError, ValueError) as e:
            self.logger.error(f"Invalid trade data: {data} - {str(e)}")
            raise

    async def shutdown(self):
        """Graceful shutdown"""
        self._shutdown_flag = True
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        try:
            for conn_id in list(self.connection_states.keys()):
                if conn_id in self.connection_states:
                    await self.adapter.shutdown(conn_id)
                else:
                    self.logger.warning(
                        f"Connection ID {conn_id} not found during shutdown. "
                        f"Current connection states: {self.connection_states.keys()}"
                    )
        except Exception as e:
            self.logger.error(f"Error during WebSocketWorker shutdown: {e}")