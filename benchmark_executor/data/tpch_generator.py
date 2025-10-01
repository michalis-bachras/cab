import asyncio
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

