# infra/db/base_repository.py
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager
from infrastructure.db.repositories.mysql_connection_pool import MySQLConnectionPool
from .DBError import (
    DBError, ConnectionError, QueryError, 
    TransactionError, RepositoryError
)

class BaseRepository:
    def __init__(self, db_config: Dict[str, Any]):
        self.pool = MySQLConnectionPool(db_config)
        self.logger = None  # Should be injected
        
    async def _execute(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        read_only: bool = False
    ):
        """Core execution method with error handling"""
        try:
            async with self.pool.get_cursor() as cursor:
                await cursor.execute(query, params or ())
                if query.strip().upper().startswith(('SELECT', 'SHOW')):
                    return await cursor.fetchall()
                elif not read_only:
                    await cursor.connection.commit()
                    return cursor.rowcount
        except Exception as e:
            self._log_error(f"Query failed: {query}", e)
            raise QueryError(f"Query execution failed: {str(e)}", e)

    async def _execute_many(
        self,
        query: str,
        values: List[Dict[str, Any]]
    ):
        """Batch execution method"""
        try:
            async with self.pool.get_cursor() as cursor:
                await cursor.executemany(query, values)
                await cursor.connection.commit()
                return cursor.rowcount
        except Exception as e:
            self._log_error(f"Batch execute failed: {query}", e)
            raise QueryError(f"Batch execution failed: {str(e)}", e)

    def _log_error(self, message: str, error: Exception):
        if self.logger:
            self.logger.error(f"{message}. Error: {str(error)}")

    @asynccontextmanager
    async def transaction(self):
        """Transaction context manager"""
        conn = None
        try:
            conn = await self.pool.get_connection()
            async with conn.cursor() as cursor:
                try:
                    yield cursor
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    raise TransactionError("Transaction failed", e)
        finally:
            if conn:
                await self.pool.release_connection(conn)