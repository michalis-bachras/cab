import asyncio
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Optional, Dict, Any
import logging
import aiofiles

from benchmark_executor.data.tpch_generator import TPCHDataGenerator
from benchmark_executor.databases.base_handler import DatabaseHandler
from benchmark_executor.databases.database_factory import DatabaseFactory
from benchmark_executor.databases.types import DatabaseType
from benchmark_executor.results.quert_result import QueryResult
from benchmark_executor.results.stream_result import StreamExecutionResult
from benchmark_executor.schemas.schema_manager import SchemaManager

logger = logging.getLogger(__name__)


class SingleStreamExecutor:
    """Executor for single query stream"""

    def __init__(self,database_type:DatabaseType,stream_file_path:str,stream_index:int,config: Dict[str, Any]):
        self.database_type = database_type
        self.stream_file_path = Path(stream_file_path)
        self.concurrency = config['concurrency']
        self.stream_id = stream_index
        self.database_handler: Optional[DatabaseHandler] = None
        self.config = config

        self.stream_definition: Optional[Dict[str, Any]] = None
        self.query_templates: Dict[int, Any] = {}
        script_dir = Path(__file__).parent
        schema_path = (script_dir / config['schema_directory']).absolute()

        self.schema_manager = SchemaManager(schema_path)
        gen_path = (script_dir / config['dbgen_path']).absolute()
        self.data_generator = TPCHDataGenerator(gen_path)
        self.temp_dir: Optional[Path] = None


    async def initialize(self):
        """
        Initialize database handler and load stream definition.
        This is called before provisioning the databases in the main process.
        This is also called in each process, so each process gets its own connection pool.
        """

        logger.info(f"Initializing stream executor for: {self.stream_id}")

        self.database_handler = DatabaseFactory.create_handler(
            database_type=self.database_type,
            config=self.config,
            stream_id = self.stream_id)
        await self.database_handler.initialize_connection_pool(self.config)
        await self._load_stream_definition()


    async def _load_stream_definition(self) -> None:
        """Load the JSON file that defines this query stream's workload pattern"""
        try:
            async with aiofiles.open(self.stream_file_path,'r') as f:
                content = await f.read()
                self.stream_definition = json.loads(content)
            # Filter refresh queries if configured
            if not self.config.get('include_refresh_queries', False):
                original_count = len(self.stream_definition['queries'])
                self.stream_definition['queries'] = [
                    q for q in self.stream_definition['queries']
                    if q['query_id'] != 23
                ]
                filtered_count = original_count - len(self.stream_definition['queries'])

                if filtered_count > 0:
                    logger.info(f"Filtered out {filtered_count} query 23 executions "
                                f"(include_refresh_queries=false)")
        except FileNotFoundError:
            logger.error(f"File {self.stream_file_path} not found.")
        except json.decoder.JSONDecodeError:
            logger.error(f"File {self.stream_file_path} could not be decoded.")


    def _setup_temp_directory(self) -> Path:
        """
        Create a temporary directory for data generation.

        Each provisioning operation gets its own temporary directory to
        ensure isolation and make cleanup simple.
        """
        if self.temp_dir is None:
            self.temp_dir = Path(tempfile.mkdtemp(prefix=f"tpch_provision_{os.getpid()}_"))
            logger.info(f"Created temporary directory: {self.temp_dir}")
        return self.temp_dir


    async def _load_table_data(self, csv_file: Path, table_name: str) -> QueryResult:
        """
        Load CSV data into a specific database table.
        """
        start_time = time.time()
        full_table_name = table_name + "_" + str(self.stream_id)
        logger.info(f"Loading {csv_file.name} into table {full_table_name}")
        load_result = await self.database_handler.load_data(csv_file, full_table_name)

        return load_result

    def get_database_name(self, database_type: DatabaseType) -> str: # Needs to go to the database handler
        """Generate a standardized database name for this stream."""
        return f"{database_type.value}_database_{self.stream_id}.db"

    def _cleanup_temp_directory(self):
        """
        Clean up temporary directory and all its contents.
        """
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")
            self.temp_dir = None


    async def provision_database(self,database_type: DatabaseType ) -> Dict[str, Any]:
        """Provision a complete database instance for a stream configuration."""

        start_time = time.time()
        try:
            await self.schema_manager.create_schema(database_handler=self.database_handler, database_type=self.database_type,stream_index=self.stream_id)
            logger.info("Generating TPC-H benchmark data...")
            temp_dir = self._setup_temp_directory()
            generated_files = await self.data_generator.generate_all_tables(
                self.stream_definition['scale_factor'], temp_dir
            )
            logger.info("Loading data into database tables...")
            load_stats = {}
            total_rows = 0

            for table_name, csv_file in generated_files.items():
                load_result = await self._load_table_data(
                    csv_file, table_name
                )
                load_stats[table_name] = {
                    'row_count': load_result.row_count,
                    'file_size': csv_file.stat().st_size,
                    'table_name_in_db': table_name + "_" + str(self.stream_id)
                }
                total_rows += load_result.row_count

            total_duration = time.time() - start_time

            results = {
                'status': 'success',
                'stream_config': {
                    'database_id': self.stream_id,
                    'scale_factor': self.stream_definition['scale_factor'],
                    'database_type': database_type.value,
                },
                'provisioning_stats': {
                    'total_duration_seconds': total_duration,
                    'total_rows_loaded': total_rows,
                    'tables_created': len(load_stats),
                    'table_details': load_stats
                },
                'database_info': {
                    'database_name': self.get_database_name(database_type),
                    'database_path': Path(self.config['base_database_path'] + str(self.stream_id)) ,
                    'table_naming_pattern': f"tablename_{self.stream_id}"
                }
            }

            return results

        except Exception as e:
            error_duration = time.time() - start_time
            error_msg = f"Failed to provision database for stream {self.stream_id}: {e}"
            logger.error(error_msg)

            if self.database_handler:
                try:
                    self.database_handler.close()
                    logger.info(f"Closed database handler for stream {self.stream_id}")
                except Exception as e:
                    logger.warning(f"Error closing database handler: {e}")



            return {
                'status': 'failed',
                'stream_config': {
                    'database_id': self.stream_id,
                    'scale_factor': self.stream_definition['scale_factor'],
                    'database_type': database_type.value
                },
                'error': str(e),
                'duration_before_failure': error_duration
            }

        finally:
            self._cleanup_temp_directory()

    def _load_query_templates(self) -> None:
        """Load the parameterized query templates"""

        table_names = ["region", "nation", "customer", "lineitem", "orders", "partsupp", "part", "supplier"]
        database_id = self.stream_id
        table_postfix = f"_{database_id}"

        if "template_path" in self.config:
            template_path = Path(self.config["template_path"])
        else:
            template_path = Path('sql_templates') # Default path for sql templates

        for query_id in range(1,24):
            template_file_path = template_path / f"{query_id}.sql"
            template = ""
            try:
                with open(template_file_path,'r') as f:
                    template = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(f"File {template_file_path} not found.")

            for table_name in table_names:
                template = template.replace(f":{table_name}",f"{table_name}{table_postfix}")

            self.query_templates[query_id] = template

        # Handle query 23 special case (multi-part query)
        if 23 in self.query_templates:
            self.query_templates[23] = self.query_templates[23].split(":split:")

        logger.debug(f"Loaded {len(self.query_templates)} SQL templates for stream {self.stream_id}")



    async def run_single_query(self,query: Dict[str, Any], idx: int, synchronized_start_time:float, stream_result: StreamExecutionResult) -> None:
        """Execute one query using this stream's dedicated connection pool"""
        logger.info(f"Running query for stream {self.stream_id} and query id {idx}")
        # Check if we are too early
        actual_start = int(time.time())*1000
        planned_start = synchronized_start_time + (query['start'] )
        start_delay = actual_start - planned_start

        # Don't start too early - maintain timing precision
        if start_delay < 0:
            await asyncio.sleep(-start_delay)
            actual_start = int(time.time())*1000 # Time that we scheduled the query
            start_delay = actual_start - planned_start

        query_template = self.query_templates[query['query_id']]

        try:
            if query['query_id'] != 23:
                query_result = await self.database_handler.execute_query(
                    query=query_template, parameters = query.get('arguments', [])
                )
            stream_result.successful_queries += 1

            query_duration_with_queue = query_result.end_time - planned_start # Time it took to schedule and execute the query

            log_entry = {
                'stream_id': self.stream_id,
                'query_id': query['query_id'],
                'start': query_result.start_time, # Time that the query started execution
                'relative_start': actual_start - synchronized_start_time,
                'query_duration': query_result.duration_seconds, # Pure execution time of the query(including result fetching)
                'query_duration_with_queue': query_duration_with_queue, # Execution time + time to schedule the query
                #'start_delay': start_delay,
                'cost': query_result.cost_dollars,
                'scanned_bytes': query_result.bytes_scanned,
                'row_count': query_result.row_count,
                'database_metadata': query_result.metadata
            }

            stream_result.query_execution_log.append(log_entry)

        except Exception as e:
            stream_result.failed_queries += 1
            logger.error(f"[{self.stream_id}:{idx}] Query {query['query_id']} failed: {e}")



    async def execute_stream(self,synchronized_start_time: float) -> StreamExecutionResult:
        """Execute all queries in the stream"""

        # Load the query templates and replace the table placeholders
        self._load_query_templates()

        execution_start = time.time()
        stream_result = StreamExecutionResult(
            stream_id=self.stream_id,
            start_time=execution_start,
            end_time=0
        )
        # Schedule all queries in this stream for concurrent execution
        # Each query will use this stream's dedicated connection pool
        tasks = []
        stream_result.total_queries = len(self.stream_definition['queries'])

        for idx, query in enumerate(self.stream_definition['queries']):
            # Calculate when this query should start in milliseconds
            # Calculate absolute timestamp when this query should start
            planned_start_timestamp = synchronized_start_time + query['start']
            #delay = (synchronized_start_time - int(time.time())*1000) + (query['start'])

            async def schedule_query(q: Dict[str, Any], i: int, planned_start_timestamp: float) -> None:
                #if d > 0:
                 #   await asyncio.sleep(d/1000) # In seconds
                current_time = int(time.time() * 1000)
                # If query is scheduled for the future, wait
                if planned_start_timestamp > current_time:
                    sleep_duration = (planned_start_timestamp - current_time) / 1000  # Convert to seconds
                    await asyncio.sleep(sleep_duration)

                await self.run_single_query(query=q, idx=i,synchronized_start_time=synchronized_start_time,stream_result=stream_result)

            task = asyncio.create_task(schedule_query(query, idx, planned_start_timestamp))
            tasks.append(task)

        # Wait for all queries in this stream to complete
        await asyncio.gather(*tasks, return_exceptions=True)

        stream_result.end_time = time.time()

        return stream_result



    def cleanup(self) -> None:
        """Close this stream's database handler and related connections"""
        if self.database_handler:
            logger.info(f"Closing dedicated connection pool for stream {self.stream_id}")
            self.database_handler.close()
            logger.info(f"Stream {self.stream_id} connection pool closed")




