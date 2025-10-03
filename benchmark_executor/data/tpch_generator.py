import asyncio
import multiprocessing
import os
import time
from pathlib import Path
from typing import Dict
import logging

logger = logging.getLogger(__name__)


class TPCHDataGenerator:

    # All TPC-H tables and their corresponding dbgen table codes
    TPCH_TABLES = {
        'region': 'r',      # Small lookup table with regions
        'nation': 'n',      # Small lookup table with nations
        'customer': 'c',    # Customer master data
        'supplier': 's',    # Supplier master data
        'part': 'P',        # Part master data
        'partsupp': 'S',    # Part-supplier relationship data
        'orders': 'O',      # Order header data
        'lineitem': 'L'     # Order line item data (usually the largest table)
    }

    def __init__(self, dbgen_path: str = "tpch-dbgen/dbgen"):
        """
        Initialize the data generator with the path to the dbgen tool.

        The dbgen tool is the official TPC-H data generator. It must be
        compiled and available at the specified path for this class to work.
        """
        self.dbgen_path = Path(dbgen_path)
        self.dbgen_folder = self.dbgen_path.parent
        self._validate_dbgen_tool()

    def _validate_dbgen_tool(self):
        """
        Ensure the dbgen tool is available and executable.
        """
        if not self.dbgen_path.exists():
            raise FileNotFoundError(
                f"dbgen tool not found at {self.dbgen_path}. "
                f"Please install TPC-H dbgen tool from: "
                f"https://github.com/michalis-bachras/tpch-dbgen\n"
                f"Expected file structure:\n"
                f"  tpch-dbgen/\n"
                f"  ├── dbgen (executable)\n"
                f"  └── dists.dss (data file)"
            )

        if not os.access(self.dbgen_path, os.X_OK):
            raise PermissionError(
                f"dbgen tool at {self.dbgen_path} is not executable. "
                f"Try running: chmod +x {self.dbgen_path}"
            )

        # Check for the required dists.dss file
        dists_file = self.dbgen_folder / "dists.dss"
        if not dists_file.exists():
            raise FileNotFoundError(
                f"Required file dists.dss not found at {dists_file}. "
                f"This file should be included with the dbgen tool."
            )


    async def generate_table_data(self, table_name: str, scale_factor: float,
                                  output_dir: Path) -> Path:
        """Generate data for a specific TPC-H table."""

        if table_name not in self.TPCH_TABLES:
            raise ValueError(f"Unknown table: {table_name}. "
                             f"Supported tables: {list(self.TPCH_TABLES.keys())}")
        logger.info(f"Generating {table_name} data at scale factor {scale_factor}")

        start_time = time.time()
        table_code = self.TPCH_TABLES[table_name]
        # Build the command line arguments for dbgen
        args = [
            str(self.dbgen_path),
            "-s", str(scale_factor),
            "-T", table_code,
            "-f",  # Force overwrite existing files
            "-b", str(self.dbgen_folder / "dists.dss")
        ]

        # Set environment variable to control where dbgen writes output
        env = os.environ.copy()
        env['DSS_PATH'] = str(output_dir)

        try:
            logger.debug(f"Running dbgen commandL {' '.join(args)}")
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=output_dir
            )

            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                error_msg = f"dbgen failed for {table_name} with return code {process.returncode}"
                if stderr:
                    error_msg += f": {stderr.decode()}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            generated_file = output_dir / f"{table_name}.tbl"
            if not generated_file.exists():
                raise FileNotFoundError(f"Expected file not generated: {generated_file}")

            os.chmod(generated_file, 0o644)

            duration = time.time() - start_time
            file_size = generated_file.stat().st_size

            logger.info(f"Generated {table_name}.tbl ({file_size:,} bytes) in {duration:.2f}s")

            return generated_file

        except Exception as e:
            logger.error(f"Data generation failed for {table_name}: {e}")
            raise



    async def generate_all_tables(self, scale_factor: float, output_dir: Path) -> Dict[str, Path]:
        """Generate data for all TPC-H tables at the specified scale factor."""

        logger.info(f"Generating all TPC-H tables at scale factor {scale_factor}")

        start_time = time.time()
        generated_files = {}
        table_order = ['region', 'nation', 'customer', 'supplier', 'part', 'partsupp', 'orders', 'lineitem']

        for table_name in table_order:
            try:
                file_path = await self.generate_table_data(table_name, scale_factor, output_dir)
                generated_files[table_name] = file_path
            except Exception as e:
                logger.error(f"Failed to generate {table_name}: {e}")
                raise RuntimeError(f"Data generation failed for {table_name}: {e}")

        total_duration = time.time() - start_time
        total_size = sum(f.stat().st_size for f in generated_files.values())

        logger.info(f"Generated all {len(generated_files)} tables "
                    f"({total_size:,} total bytes) in {total_duration:.2f}s")

        return generated_files

    def _determine_chunk_strategy(self, scale_factor: float) -> Dict[str, int]:

        try:
            cpu_count = multiprocessing.cpu_count()
        except NotImplementedError:
            # Fallback if cpu_count() isn't available on this platform
            cpu_count = os.cpu_count() or 4  # Conservative default
        logger.info(f"Detected {cpu_count} CPU cores for parallel processing")

        #if scale_factor < 0.5:
         #   logger.info("Scale factor < 1: disabling chunking (tables too small)")
         #   return {table: 1 for table in self.TPCH_TABLES.keys()}

        max_total_chunks = int(cpu_count * 1.5)

        # Start with base chunk counts that scale with data size
        # For small datasets or few cores, we'll reduce these further below
        chunk_multiplier = max(2, int(scale_factor ** 0.5))

        base_strategy = {
            'region': 1,      # Never chunk - too tiny
            'nation': 1,      # Never chunk - too tiny
            'supplier': 1,    # Too small to benefit from chunking
            'customer': min(chunk_multiplier, 4),
            'part': min(chunk_multiplier, 4),
            'partsupp': min(chunk_multiplier * 2, 8),
            'orders': min(chunk_multiplier * 2, 8),
            'lineitem': min(chunk_multiplier * 4, 16)
        }

        # Calculate total chunks this strategy would create
        total_base_chunks = sum(base_strategy.values())

        # If our base strategy exceeds our CPU budget, we need to scale it back
        if total_base_chunks > max_total_chunks:
            scale_factor_reduction = max_total_chunks / total_base_chunks

            logger.info(f"Base strategy would create {total_base_chunks} chunks, "
                        f"but CPU budget is {max_total_chunks}. Scaling back by "
                        f"{scale_factor_reduction:.2f}x")

            # Apply the reduction, ensuring we never go below 1 chunk per table
            final_strategy = {
                table: max(1, int(chunks * scale_factor_reduction))
                for table, chunks in base_strategy.items()
            }
            # After rounding down, we might have some unused chunk slots
            # Give these to the largest table (lineitem) if it would benefit
            actual_total = sum(final_strategy.values())
            remaining_budget = max_total_chunks - actual_total

            if remaining_budget > 0 and final_strategy['lineitem'] < base_strategy['lineitem']:
                # Add the remaining budget to lineitem, but don't exceed its base allocation
                additional_chunks = min(
                    remaining_budget,
                    base_strategy['lineitem'] - final_strategy['lineitem']
                )
                final_strategy['lineitem'] += additional_chunks
                logger.info(f"Allocated {additional_chunks} additional chunks to lineitem "
                            f"(largest table)")

        else:
            # We're within budget, use the base strategy as-is
            final_strategy = base_strategy
            logger.info(f"Base strategy creates {total_base_chunks} chunks, "
                        f"within CPU budget of {max_total_chunks}")

        final_total = sum(final_strategy.values())
        logger.info(f"Final chunk strategy for SF {scale_factor} with {cpu_count} cores: "
                    f"{final_strategy} (total: {final_total} chunks)")

        return final_strategy




    async def generate_table_chunk(self,table_name:str, scale_factor:float,output_dir:Path,chunk_count:int, step:int) -> Path:
        """Generate a single chunk of a TPC-H table."""

        if table_name not in self.TPCH_TABLES:
            raise ValueError(f"Unknown table: {table_name}. "
                             f"Supported tables: {list(self.TPCH_TABLES.keys())}")

        if step < 1 or step > chunk_count:
            raise ValueError(f"Step must be between 1 and {chunk_count}, got {step}")

        logger.info(f"Generating {table_name} chunk {step}/{chunk_count} "
                    f"at scale factor {scale_factor}")

        start_time = time.time()
        table_code = self.TPCH_TABLES[table_name]

        args = [
            str(self.dbgen_path),
            "-s", str(scale_factor),
            "-T", table_code,
            "-f",
            "-b", str(self.dbgen_folder / "dists.dss"),
            "-C", str(chunk_count),
            "-S", str(step)
        ]

        # Set environment variable so dbgen knows where to write output
        env = os.environ.copy()
        env['DSS_PATH'] = str(output_dir)

        try:
            logger.debug(f"Running dbgen commandL {' '.join(args)}")
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=output_dir
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                error_msg = f"dbgen failed for {table_name} chunk {step}/{chunk_count} " \
                            f"with return code {process.returncode}"
                if stderr:
                    error_msg += f": {stderr.decode()}"
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            generated_file = output_dir / f"{table_name}.tbl.{step}"
            if not generated_file.exists():
                raise FileNotFoundError(
                    f"Expected chunk file not generated: {generated_file}"
                )
            os.chmod(generated_file, 0o644)

            duration = time.time() - start_time
            file_size = generated_file.stat().st_size

            logger.info(f"Generated {table_name}.tbl.{step} chunk "
                        f"({file_size:,} bytes) in {duration:.2f}s")

            return generated_file

        except Exception as e:
            logger.error(f"Chunk generation failed for {table_name} "
                         f"chunk {step}/{chunk_count}: {e}")
            raise


    async def generate_table_with_chunks(self, table_name: str, scale_factor: float,
                                         output_dir: Path, chunk_count: int = 1) -> list[Path]:
        """Generate a table, optionally split into multiple chunks for parallel processing."""

        if chunk_count == 1:
            logger.info(f"Generating {table_name} as single file (no chunking)")
            file_path = await self.generate_table_data(table_name, scale_factor, output_dir)
            return [file_path]
        else:
            logger.info(f"Generating {table_name} split into {chunk_count} chunks")
            chunk_tasks = [
                self.generate_table_chunk(
                    table_name=table_name,
                    scale_factor=scale_factor,
                    output_dir=output_dir,
                    chunk_count=chunk_count,
                    step=step
                )
                for step in range(1, chunk_count + 1)  # Steps are 1-indexed
            ]
            chunk_files = await asyncio.gather(*chunk_tasks)

            logger.info(f"Successfully generated all {chunk_count} chunks for {table_name}")

            return list(chunk_files)


    async def generate_all_tables_parallel(self, scale_factor: float, output_dir: Path,
                                           chunk_strategy: Dict[str, int] = None) -> Dict[str, list[Path]]:

        logger.info(f"Generating all TPC-H tables at scale factor {scale_factor} with parallelization")
        if chunk_strategy is None:
            chunk_strategy = self._determine_chunk_strategy(scale_factor)

        logger.info(f"Using chunk strategy: {chunk_strategy}")

        start_time = time.time()

        # Each task will handle generating that table (potentially with chunks)
        table_tasks = {
            table_name: self.generate_table_with_chunks(
                table_name=table_name,
                scale_factor=scale_factor,
                output_dir=output_dir,
                chunk_count=chunk_strategy.get(table_name, 1)
            )
            for table_name in self.TPCH_TABLES.keys()
        }

        # Execute all table generation tasks in parallel
        # This means different tables generate simultaneously
        # AND within each table, chunks (if any) also generate simultaneously
        results = await asyncio.gather(*table_tasks.values(), return_exceptions=True)

        generated_files = {}
        for table_name, result in zip(table_tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(f"Failed to generate {table_name}: {result}")
                raise RuntimeError(f"Data generation failed for {table_name}: {result}")
            generated_files[table_name] = result

        total_duration = time.time() - start_time
        total_files = sum(len(files) for files in generated_files.values())
        total_size = sum(
            f.stat().st_size
            for file_list in generated_files.values()
            for f in file_list
        )

        logger.info(f"Generated all {len(generated_files)} tables "
                    f"({total_files} total files, {total_size:,} bytes) "
                    f"in {total_duration:.2f}s")

        return generated_files


