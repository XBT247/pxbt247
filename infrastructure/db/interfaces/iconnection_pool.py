from abc import ABC, abstractmethod
from typing import Optional, AsyncContextManager
from aiomysql import Connection, Cursor, Pool

class IConnectionPool(ABC):
    """Abstract base class for database connection pools"""
    
    @abstractmethod
    async def get_connection(self) -> AsyncContextManager[Connection]:
        """Acquire a connection from the pool"""
        pass
        
    @abstractmethod
    async def get_cursor(self) -> AsyncContextManager[Cursor]:
        """Acquire a cursor from the pool"""
        pass
        
    @abstractmethod
    async def close(self) -> None:
        """Close all connections in the pool"""
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if pool is initialized and healthy"""
        pass