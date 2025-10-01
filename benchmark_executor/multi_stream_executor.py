import asyncio
import os
import re
import sys
import time
from typing import List, Any, Dict

import yaml

from benchmark_executor.databases.types import DatabaseType
import logging

from benchmark_executor.results.stream_result import StreamExecutionResult
from benchmark_executor.single_stream_executor import SingleStreamExecutor

logger = logging.getLogger(__name__)
class MultiStreamExecutor:
    """Coordinator ofr multi-stream concurrent execution of CAB query streams"""

    def __init__(self, database_type:DatabaseType):

        self.database_type = database_type
        self.stream_executors: List[SingleStreamExecutor] = []
        self.database_config: Dict[str, Any] = {}
        self.config_dir = "databases/configs"
        self.loading_process = True

        logger.info(f"Multi-stream Executor for {database_type.value}")


    async def initialize_streams(self,stream_file_paths:List[str]):
        logger.info(f"Initializing {len(stream_file_paths)} streams")
        working_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(working_dir,self.config_dir,self.database_type.value+".yaml")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as config_file:
                    config = yaml.safe_load(config_file)
                    self.database_config = config
            except yaml.YAMLError as e:
                raise ValueError(f"YAML parsing error in {config_path}: {e}")
            except Exception as e:
                raise ValueError(f"Error parsing {config_path}: {e}")
        else:
            logger.info(f"No config file found at {config_path}")
            sys.exit(1)

        self.stream_executors = [
            SingleStreamExecutor(
                database_type = self.database_type,
                stream_file_path = stream_path,
                stream_index = int(re.search(r"query_stream_(\d+)\.json", os.path.basename(stream_path)).group(1)),
                config=self.database_config
            )
            for stream_path in stream_file_paths
        ]

        for stream_executor in self.stream_executors:
            await stream_executor.initialize()

        if self.loading_process:
            results = []
            load_start_time = time.time()
            for stream_executor in self.stream_executors:
                result = await stream_executor.provision_database(self.database_type)
                results.append(result)
            load_duration = time.time() - load_start_time



    async def execute_all_streams_synchronized(self) -> List[StreamExecutionResult]:
        """Execute all streams with synchronized timing, each using its own connection pool."""

        if not self.stream_executors:
            raise ValueError("No streams initialized for execution")

        buffer_seconds = 3000
        synchronized_start_time = int(time.time() * 1000) + buffer_seconds

        execution_tasks = [
            executor.execute_stream(synchronized_start_time)
            for executor in self.stream_executors
        ]

        try:
            # Wait for all streams to complete their workloads
            results = await asyncio.gather(*execution_tasks, return_exceptions=True)
            successful_results = []
            failed_streams = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    stream_id = self.stream_executors[i].stream_id
                    failed_streams.append((stream_id, result))
                    logger.error(f"Stream {stream_id} failed: {result}")
                else:
                    successful_results.append(result)

            if failed_streams:
                logger.warning(f"{len(failed_streams)} streams failed during execution")

            total_queries_executed = sum(r.total_queries for r in successful_results)
            total_successful_queries = sum(r.successful_queries for r in successful_results)

            logger.info(f"Multi-stream execution completed:")
            logger.info(f"  - Successful streams: {len(successful_results)}/{len(self.stream_executors)}")
            logger.info(f"  - Total queries executed: {total_queries_executed}")
            logger.info(f"  - Overall success rate: {total_successful_queries}/{total_queries_executed}")
            logger.info(f"  - Each stream used its own dedicated connection pool")

            return successful_results

        finally:
            logger.info("Cleaning up all stream connection pools...")
            cleanup_tasks = [executor.cleanup() for executor in self.stream_executors]
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            logger.info("All connection pools closed")


    async def run_benchmark(self,stream_file_paths:List[str]) -> List[Any]:
        """Complete benchmark workflow"""

        try:
            await self.initialize_streams(stream_file_paths)
            results = await self.execute_all_streams_synchronized()
            return results
        except Exception as e:
            logger.error(f"Multi-stream benchmark failed: {e}")
            raise




