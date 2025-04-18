# infra/db/mysql/connection_pool.py
import aiomysql
from typing import Optional, AsyncIterator
from contextlib import asynccontextmanager
from aiomysql import Connection, Cursor, Pool
from infrastructure.db.interfaces.iconnection_pool import IConnectionPool

class MySQLConnectionPool(IConnectionPool):
    _instance: Optional['MySQLConnectionPool'] = None
    _pool: Optional[Pool] = None
    
    def __new__(cls, db_config: dict):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(db_config)
        return cls._instance
        
    def _initialize(self, db_config: dict):
        """Initialize with configuration but don't connect yet"""
        self.db_config = {
            'host': db_config['host'],
            'port': db_config['port'],
            'user': db_config['user'],
            'password': db_config['password'],
            'db': db_config['database'],
            'minsize': db_config.get('minsize', 1),
            'maxsize': db_config.get('maxsize', 10),
            'pool_recycle': db_config.get('pool_recycle', 300),
            'autocommit': True,
            'cursorclass': aiomysql.DictCursor
        }
        self._pool = None
        
    async def _ensure_pool(self) -> None:
        """Lazy initialization of connection pool"""
        if self._pool is None or self._pool._closed:
            self._pool = await aiomysql.create_pool(**self.db_config)
            
    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[Connection]:
        """Acquire a connection with context management"""
        await self._ensure_pool()
        async with self._pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                await conn.rollback()
                raise
            finally:
                # Connection automatically returns to pool
                pass
                
    @asynccontextmanager
    async def get_cursor(self) -> AsyncIterator[Cursor]:
        """Acquire a cursor with context management"""
        async with self.get_connection() as conn:
            async with conn.cursor() as cursor:
                yield cursor
                await conn.commit()
                
    async def close(self) -> None:
        """Close all connections in the pool"""
        if self._pool and not self._pool._closed:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            
    def is_connected(self) -> bool:
        """Check if pool is initialized and healthy"""
        return self._pool is not None and not self._pool._closed
        
    async def __aenter__(self):
        await self._ensure_pool()
        return self
        
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()