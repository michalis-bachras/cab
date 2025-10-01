from typing import List, Dict, Any


class StreamExecutionResult:
    """Complete results from executing a single query stream."""
    def __init__(self, stream_id: int, start_time: float, end_time: float):
        self.stream_id = stream_id
        self.start_time = start_time
        self.end_time = end_time
        self.query_execution_log: List[Dict[str, Any]] = []
        self.total_queries = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.total_cost = 0.0
        self.total_scanned_bytes = 0

    @property
    def duration(self) -> float:
        """Calculate total execution time for this stream."""
        return self.end_time - self.start_time

    @property
    def success_rate(self) -> float:
        """Calculate percentage of successful queries."""
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100