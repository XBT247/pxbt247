# infra/db/error_handling.py
from typing import Optional

class DBError(Exception):
    """Base database error"""
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        self.message = message
        self.original_error = original_error
        super().__init__(message)

class ConnectionError(DBError):
    """Connection-related errors"""
    pass

class QueryError(DBError):
    """Query execution errors"""
    pass

class TransactionError(DBError):
    """Transaction-related errors"""
    pass

class PoolError(DBError):
    """Connection pool errors"""
    pass

class RepositoryError(DBError):
    """Repository operation errors"""
    pass