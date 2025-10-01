from typing import List, Optional, Dict, Any

from benchmark_executor.databases.base_handler import DatabaseHandler
from benchmark_executor.databases.duckdb import DuckDBHandler
from benchmark_executor.databases.types import DatabaseType


class DatabaseFactory:

    _database_implementations = {
        DatabaseType.DUCKDB:DuckDBHandler
    }

    @classmethod
    def create_handler(cls,database_type: DatabaseType, config: Dict[str, Any],stream_id: Optional[int] = None) -> DatabaseHandler:
        if database_type not in cls._database_implementations:
            raise Exception(f'Database type {database_type} not supported')

        database_class = cls._database_implementations[database_type]
        handler = database_class(stream_id=stream_id, config=config)
        return handler

    @classmethod
    def get_supported_database_types(cls) -> List[str]:
        return [db_type.value for db_type in cls._database_implementations.keys()]