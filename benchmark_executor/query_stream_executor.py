import asyncio
import os
import pathlib
import sys
import logging

from benchmark_executor.databases.config_manager import ConfigurationManager
from benchmark_executor.databases.database_factory import DatabaseFactory
from benchmark_executor.databases.types import DatabaseType
from benchmark_executor.multi_stream_executor import MultiStreamExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_stream_files_in_dir(directory_path):
    dir = pathlib.Path(directory_path)
    stream_files = []
    if not dir.exists():
        print(f"Error: Directory '{directory_path}' does not exist.")
        return []
    for file in dir.glob("*.json"):
        if file.is_file():
            stream_files.append(file)

    return sorted([str(file) for file in stream_files])


async def main():

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python query_stream_executor.py <database_type>")
        print("  python query_stream_executor.py <database_type> <stream_file1> <stream_file2> ...")
        sys.exit(1)


    current_dir = os.getcwd()
    query_streams_dir = os.path.join(current_dir, "benchmark-gen/query_streams")
    #query_streams_dir = os.path.join(current_dir,"query_streams")
    query_streams_dir = os.path.abspath(query_streams_dir)
    database_type_str = sys.argv[1]
    query_streams = []
    print(f"Database type is {database_type_str}")
    if len(sys.argv) == 2:
        query_streams = find_stream_files_in_dir(query_streams_dir)
        if len(query_streams) == 0:
            print("No streams found, exiting.")
            sys.exit(1)
    else:
        query_streams = sys.argv[2:]
        for query_stream in query_streams:
            query_streams.append(os.path.join(query_streams_dir,query_stream))

    concurrency_per_stream = 10
    total_connections = len(query_streams) * concurrency_per_stream

    try:
        database_type = DatabaseType(database_type_str)
    except ValueError:
        print(f"Unsupported database type: {database_type_str}")
        print(f"Available types: {', '.join(DatabaseFactory.get_supported_database_types())}")
        sys.exit(1)

    try:
        executor = MultiStreamExecutor(
            database_type=database_type
        )
        logger.info(f"Starting multi-stream benchmark with {database_type_str}")
        logger.info(f"Each stream will load configuration from configs/{database_type_str}.yaml")

        #results = await executor.run_benchmark(query_streams)
        results = await executor.run_benchmark_with_processes(query_streams)


    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)










if __name__ == "__main__":
    asyncio.run(main())