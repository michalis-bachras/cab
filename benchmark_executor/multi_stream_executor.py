import asyncio
import os
import re
import sys
import time
from multiprocessing import Queue, Process
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

      #  for stream_executor in self.stream_executors:
      #      await stream_executor.initialize()

       # if self.loading_process:
       #     results = []
      #      load_start_time = time.time()
      #      for stream_executor in self.stream_executors:
      #          result = await stream_executor.provision_database(self.database_type)
      #          results.append(result)
       #     load_duration = time.time() - load_start_time


    async def provision_all_databases(self) -> List[Dict[str, Any]]:
        """Provision all databases sequentially"""

        if not self.loading_process:
            logger.info("Skipping database provisioning")
            return []

        logger.info(f"Starting database provisioning for {len(self.stream_executors)} streams")
        results = []
        load_start_time = time.time()

        # Initialize each executor and provision its database
        for stream_executor in self.stream_executors:
            logger.info(f"Provisioning database for stream {stream_executor.stream_id}")

            # Initialize connection pool for this stream
            await stream_executor.initialize()

            # Provision the database (create tables, load data)
            result = await stream_executor.provision_database(self.database_type)
            results.append(result)

            # Clean up the connection pool after provisioning
            # The execution processes will create their own pools later
            stream_executor.cleanup()

        load_duration = time.time() - load_start_time
        logger.info(f"All databases provisioned in {load_duration:.2f} seconds")

        return results

    @staticmethod
    def _run_stream_in_process(database_type: DatabaseType,
                               stream_file_path: str,
                               stream_index: int,
                               config: Dict[str, Any],
                               synchronized_start_time: float,
                               result_queue: Queue):

        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s - [Process-{stream_index}] - %(levelname)s - %(message)s'
        )
        process_logger = logging.getLogger(f"stream_{stream_index}")

        try:
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)

            process_logger.info(f"Process started for stream {stream_index}")
            # Create the stream executor in this process
            executor = SingleStreamExecutor(
                database_type=database_type,
                stream_file_path=stream_file_path,
                stream_index=stream_index,
                config=config
            )

            # Run the async initialization and execution
            async def execute():
                # Initialize connection pool for this process
                await executor.initialize()

                # Execute the stream
                stream_result = await executor.execute_stream(synchronized_start_time)

                # Clean up
                executor.cleanup()

                return stream_result

            # Execute in this process's event loop
            stream_result = loop.run_until_complete(execute())


            # Convert result to a dictionary for inter-process communication
            result_dict = {
                'stream_id': stream_result.stream_id,
                'start_time': stream_result.start_time,
                'end_time': stream_result.end_time,
                'total_queries': stream_result.total_queries,
                'successful_queries': stream_result.successful_queries,
                'failed_queries': stream_result.failed_queries,
                'query_execution_log': stream_result.query_execution_log
            }

            result_queue.put(('success', stream_index, result_dict))
            process_logger.info(f"Stream {stream_index} completed successfully")

        except Exception as e:
            process_logger.error(f"Stream {stream_index} failed: {e}")
            result_queue.put(('error', stream_index, str(e)))
        finally:
            loop.close()


    async def execute_all_streams_in_processes(self) -> List[StreamExecutionResult]:
        """
        Execute all streams in separate processes with synchronized timing.
        Each process has its own event loop and runs independently.
        """

        if not self.stream_executors:
            raise ValueError("No streams initialized for execution")

        buffer_seconds = 3000
        synchronized_start_time = int(time.time() * 1000) + buffer_seconds

        logger.info(f"Starting {len(self.stream_executors)} streams in separate processes")
        logger.info(f"Synchronized start time: {synchronized_start_time} ms")

        # Create a queue for collecting results from processes
        result_queue = Queue()

        # Create and start a process for each stream
        processes = []
        for executor in self.stream_executors:
            process = Process(
                target=self._run_stream_in_process,
                args=(
                    self.database_type,
                    str(executor.stream_file_path),
                    executor.stream_id,
                    self.database_config,
                    synchronized_start_time,
                    result_queue
                )
            )
            processes.append(process)
            process.start()
            logger.info(f"Started process for stream {executor.stream_id} (PID: {process.pid})")

        # Wait for all processes to complete
        for process in processes:
            process.join()

        # Collect results from the queue
        successful_results = []
        failed_streams = []

        while not result_queue.empty():
            status, stream_id, data = result_queue.get()

            if status == 'success':
                # Reconstruct StreamExecutionResult from dictionary
                result = StreamExecutionResult(
                    stream_id=data['stream_id'],
                    start_time=data['start_time'],
                    end_time=data['end_time']
                )
                result.total_queries = data['total_queries']
                result.successful_queries = data['successful_queries']
                result.failed_queries = data['failed_queries']
                result.query_execution_log = data['query_execution_log']
                successful_results.append(result)
            else:
                failed_streams.append((stream_id, data))
                logger.error(f"Stream {stream_id} failed: {data}")

        # Log summary
        if failed_streams:
            logger.warning(f"{len(failed_streams)} streams failed during execution")

        total_queries_executed = sum(r.total_queries for r in successful_results)
        total_successful_queries = sum(r.successful_queries for r in successful_results)

        logger.info(f"Multi-stream execution completed:")
        logger.info(f"  - Successful streams: {len(successful_results)}/{len(self.stream_executors)}")
        logger.info(f"  - Total queries executed: {total_queries_executed}")
        logger.info(f"  - Overall success rate: {total_successful_queries}/{total_queries_executed}")
        logger.info(f"  - Each stream ran in its own process with dedicated connection pool")

        return successful_results


    async def run_benchmark_with_processes(self, stream_file_paths: List[str]) -> List[Any]:
        """
        Complete benchmark workflow:
        1. Initialize stream configurations
        2. Provision all databases (sequential, main process)
        3. Execute all streams (parallel, separate processes)
        """

        try:
            # Step 1: Initialize stream executors
            await self.initialize_streams(stream_file_paths)

            # Step 2: Provision all databases in the main process
            # This ensures all database files are created before processes start
            provision_results = await self.provision_all_databases()

            # Step 3: Execute all streams in separate processes
            # Each process will create its own connection pool
            execution_results = await self.execute_all_streams_in_processes()

            return execution_results
        except Exception as e:
            logger.error(f"Multi-stream benchmark failed: {e}")
            raise














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




