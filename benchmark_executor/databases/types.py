from enum import Enum


class DatabaseType(Enum):
    """
    Enumeration of supported database types.
    """
    POSTGRESQL = "postgresql"
    DUCKDB = "duckdb"
