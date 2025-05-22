import asyncio
from typing import Callable, Any, Optional
from functools import wraps
import structlog
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession


class DatabaseErrorHandler:
    """Handle database errors with retry logic and rollback"""

    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = structlog.get_logger().bind(component="db_error_handler")

    async def execute_with_retry(self, session: AsyncSession, operation: Callable, *args, **kwargs) -> Any:
        """Execute database operation with retry logic"""
        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                result = await operation(session, *args, **kwargs)
                return result

            except (DisconnectionError, OperationalError) as e:
                last_exception = e

                if attempt < self.max_retries:
                    self.logger.warning(
                        "Database connection error, retrying",
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        error=str(e)
                    )

                    # Rollback current transaction
                    try:
                        await session.rollback()
                    except Exception:
                        pass

                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    self.logger.error(
                        "Max retries exceeded for database operation",
                        max_retries=self.max_retries,
                        error=str(e)
                    )
                    break

            except SQLAlchemyError as e:
                last_exception = e

                # Rollback transaction for SQLAlchemy errors
                try:
                    await session.rollback()
                except Exception:
                    pass

                self.logger.error("SQLAlchemy error in database operation", error=str(e))
                break

            except Exception as e:
                last_exception = e

                # Rollback for any other error
                try:
                    await session.rollback()
                except Exception:
                    pass

                self.logger.error("Unexpected error in database operation", error=str(e))
                break

        # If we get here, all retries failed
        if last_exception:
            raise last_exception

        return None

    def with_retry(self, max_retries: Optional[int] = None, retry_delay: Optional[float] = None):
        """Decorator for database operations with retry logic"""

        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Extract session from args or kwargs
                session = None
                for arg in args:
                    if isinstance(arg, AsyncSession):
                        session = arg
                        break

                if not session:
                    session = kwargs.get('session')

                if not session:
                    raise ValueError("No AsyncSession found in arguments")

                retries = max_retries or self.max_retries
                delay = retry_delay or self.retry_delay

                handler = DatabaseErrorHandler(retries, delay)
                return await handler.execute_with_retry(session, func, *args, **kwargs)

            return wrapper

        return decorator

    async def safe_commit(self, session: AsyncSession) -> bool:
        """Safely commit transaction with error handling"""
        try:
            await session.commit()
            return True
        except Exception as e:
            self.logger.error("Failed to commit transaction", error=str(e))
            try:
                await session.rollback()
            except Exception as rollback_error:
                self.logger.error("Failed to rollback transaction", error=str(rollback_error))
            return False

    async def safe_rollback(self, session: AsyncSession) -> bool:
        """Safely rollback transaction"""
        try:
            await session.rollback()
            return True
        except Exception as e:
            self.logger.error("Failed to rollback transaction", error=str(e))
            return False

    async def execute_in_transaction(self, session: AsyncSession, operations: list) -> bool:
        """Execute multiple operations in a single transaction"""
        try:
            for operation in operations:
                if callable(operation):
                    await operation(session)
                else:
                    await session.execute(operation)

            await session.commit()
            return True

        except Exception as e:
            self.logger.error("Transaction failed", error=str(e))
            await self.safe_rollback(session)
            return False


# Global instance
db_error_handler = DatabaseErrorHandler()
