
from dataclasses import dataclass
from typing import Dict, List, Any, Optional

@dataclass
class QueryResult:
    """
    Standardized container for query execution results and performance metrics.

    Every database handler returns results in this format, ensuring consistent
    measurement across different database types and connection pools.
    """
    data: List[Dict[str, Any]]           # The actual query results
    duration_seconds: float              # Pure query execution time
    start_time: float
    end_time: float
    row_count: int                      # Number of rows returned
    cost_dollars: float = 0.0           # Cost for cloud databases
    bytes_scanned: int = 0              # Amount of data scanned
    bytes_processed: int = 0            # Amount of data processed
    metadata: Optional[Dict[str, Any]] = None  # Database-specific extra info

    def __post_init__(self):
        """Ensure metadata is always a dictionary, never None."""
        if self.metadata is None:
            self.metadata = {}
