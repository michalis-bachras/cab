import asyncio
import os.path
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

import yaml
import duckdb

from benchmark_executor.databases.base_handler import DatabaseHandler
from benchmark_executor.results.quert_result import QueryResult

logger = logging.getLogger(__name__)


class DuckDBHandler(DatabaseHandler):
    def __init__(self, stream_id: Optional[int] = None, config: Optional[Dict[str, Any]] = None):
        super().__init__(stream_id=stream_id, config=config)

        self.connection_pool: List[Any] = []
        self.pool_lock = asyncio.Lock()
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.db_path: Optional[str] = None



    def get_connection(self):
        pass

    def _fill_sql_template(self, sql: str, parameters: List[Any]) -> str:

        if not parameters:
            return sql

        result = sql

        # Process parameters in reverse order like JavaScript (to avoid :10 replacing :1)
        for i in range(len(parameters) - 1, -1, -1):
            value = parameters[i]

            if value is None:
                continue

            placeholder = f":{i + 1}"  # :1, :2, :3, etc. (1-based indexing)

            if isinstance(value, (int, float)):
                replacement = str(value)
            elif isinstance(value, str):
                # Escape single quotes and wrap in quotes
                escaped_value = value.replace("'", "''")
                replacement = f"'{escaped_value}'"
            else:
                # For other types, convert to string and quote
                replacement = f"'{str(value)}'"

            result = result.replace(placeholder, replacement)
        return result



    async def execute_query(self, query: str, parameters: List[Any] = None) -> QueryResult:
        filled_sql = self._fill_sql_template(query, parameters or {})
        # semaphore controls concurrency
        await self.semaphore.acquire()

        # safely access the connection pool
        async with self.pool_lock:
            if not self.connection_pool:
                raise RuntimeError("No available DuckDB connections in this handler's pool")

            # Remove one connection from the pool for exclusive use
            connection = self.connection_pool.pop()

        try:
            start_execution_time = int(time.time())*1000
            result = connection.execute(filled_sql)
            rows = result.fetchall()
            end_execution_time = start_execution_time = int(time.time())*1000
            pure_database_execution_time = end_execution_time - start_execution_time
            columns = [desc[0] for desc in result.description] if result.description else []

            data = [dict(zip(columns, row)) for row in rows]

            return QueryResult(
                data=data,
                duration_seconds=pure_database_execution_time,
                start_time=start_execution_time,
                end_time=end_execution_time,
                row_count=len(rows),
                cost_dollars=0.0,  # DuckDB doesn't have monetary cost
                bytes_scanned=0,   # Would need profiling for exact metrics
                bytes_processed=0,
                metadata={
                    'database_type': 'duckdb',
                    'database_path': self.db_path,
                    'sql_length': len(filled_sql),
                    'handler_pool_size': len(self.connection_pool) + 1,  # +1 for connection in use
                    'query_execution_method': 'dedicated_pool'
                }
            )

        except Exception as e:
            end_time = time.time()
            logger.error(f"DuckDB query failed after {end_time - start_execution_time:.3f}s: {e}")
            raise RuntimeError(f"DuckDB query execution failed: {e}")

        finally:
            async with self.pool_lock:
                self.connection_pool.append(connection)
            self.semaphore.release()

    def close(self):
        logger.info(f"Closing DuckDB connection pool with {len(self.connection_pool)} connections")
        for conn in self.connection_pool:
            conn.close()
        self.connection_pool.clear()
        self.semaphore = None

    async def initialize_connection_pool(self, config) -> None:
        """Create dedicated DuckDB connection pool for this handler."""

        self.db_path = self._get_database_path_for_stream()
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        pool_size = config.get("concurrency", 10)
        self.semaphore = asyncio.Semaphore(pool_size)
        for i in range(pool_size):
            conn = duckdb.connect(database = self.db_path)

            # Apply DuckDB-specific settings if configured
            duckdb_settings = config.get('duckdb_settings', {})
            for setting, value in duckdb_settings.items():
                conn.sql(f"SET {setting} = '{value}'")

            self.connection_pool.append(conn)
        logger.info(f"DuckDB connection pool initialized with {len(self.connection_pool)} connections")

    def _get_database_path_for_stream(self) -> str:
        """
        Get the appropriate database path based on configuration and stream_id
        Returns single database path or stream-specific path
        """

        if not self.config:
            raise ValueError("Configuration not loaded")

        single_database_path = self.config.get('database_path',"")
        base_database_path = self.config.get('base_database_path',"")
        if single_database_path:
            return single_database_path
        elif base_database_path:
            if self.stream_id is None:
                raise ValueError("Stream ID is required")

            base_database_path = self.config.get('base_database_path',"database.duckdb")

            path = Path(base_database_path)
            stem = path.stem
            suffix = path.suffix
            parent = path.parent
            database_specific_path = parent.joinpath(f"{stem}_{self.stream_id}{suffix}.db")
            return str(database_specific_path)
        else:
            raise ValueError("Neither database_path nor base_database_path configured")


    async def execute_ddl(self,sql:str) -> QueryResult:
        """
        Execute a DDL/DML statement that doesn't return a result set.
        Use this for CREATE, INSERT, COPY, UPDATE, DELETE, etc.

        Returns a dictionary with execution metadata instead of a QueryResult.
        """

        await self.semaphore.acquire()
        async with self.pool_lock:
            if not self.connection_pool:
                raise RuntimeError("No available DuckDB connections in this handler's pool")

            connection = self.connection_pool.pop()

        try:
            start_execution_time = int(time.time())*1000
            result = connection.execute(sql)
            end_execution_time = int(time.time() * 1000)
            duration = end_execution_time - start_execution_time

            logger.info(f"DDL/DML executed successfully in {duration}ms")

            return QueryResult(
                data=[],
                duration_seconds=duration,
                start_time=start_execution_time,
                end_time=end_execution_time,
                row_count=0,
                cost_dollars=0.0,  # DuckDB doesn't have monetary cost
                bytes_scanned=0,   # Would need profiling for exact metrics
                bytes_processed=0,
                metadata={
                    'database_type': 'duckdb',
                    'database_path': self.db_path,
                    'handler_pool_size': len(self.connection_pool) + 1,  # +1 for connection in use
                    'query_execution_method': 'dedicated_pool'
                }
            )

        except Exception as e:
            end_time = int(time.time() * 1000)
            duration = end_time - start_execution_time

            # Log the error with context
            logger.error(f"DuckDB DDL/DML failed after {duration}ms: {e}")
            logger.error(f"Failed SQL (first 200 chars): {sql[:200]}")

            # Re-raise with additional context
            raise RuntimeError(f"DuckDB DDL/DML execution failed: {e}") from e

        finally:
            async with self.pool_lock:
                self.connection_pool.append(connection)
            self.semaphore.release()




    async def load_data(self,csv_file: Path, table_name: str):
        copy_sql = f"""
                COPY {table_name} 
                FROM '{csv_file}' 
                (DELIMITER '|', HEADER false)
            """
        try:
            result = await self.execute_ddl(copy_sql)
            return result
        except RuntimeError as e:
            error_msg = str(e)
            raise RuntimeError(f"Failed to load {csv_file.name} into {table_name}: {error_msg}") from e
        except Exception as e:
            logger.error(f"Unexpected error loading {csv_file.name}: {e}")
            raise








