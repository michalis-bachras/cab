import re
from pathlib import Path
import logging
from typing import List

from benchmark_executor.databases.types import DatabaseType

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schema creation using external SQL files."""

    def __init__(self,schema_dir:Path):
        self.schema_dir = Path(schema_dir)
        if not self.schema_dir.exists():
            raise FileNotFoundError(f"Schema directory not found: {schema_dir}")

        logger.info(f"Initialized SchemaManager with schema directory: {schema_dir}")



    def _extract_table_name_from_create_statement(self, statement: str) -> str:
        statement = statement.strip().upper()
        pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)'
        match = re.search(pattern, statement, re.IGNORECASE)

        if not match:
            raise ValueError(f"Could not find table name in: {statement}")

        table_name = match.group(1)

        # Remove any trailing parenthesis
        if '(' in table_name:
            table_name = table_name[:table_name.index('(')]

        # Handle schema qualification if needed
        if '.' in table_name:
            table_name = table_name.split('.')[-1]  # Get just the table name

        return table_name.lower()


    def _transform_create_table_statement(self, statement: str, database_id: int) -> str:
        cleaned_statement = statement.strip()

        try:
            original_table_name = self._extract_table_name_from_create_statement(cleaned_statement)
            new_table_name = f"{original_table_name}_{database_id}"
            transformed_statement = cleaned_statement.replace(original_table_name, new_table_name, 1)
            logger.debug(f"Transformed table: {original_table_name} -> {new_table_name}")
            return transformed_statement
        except Exception as e:
            logger.error(f"Failed to transform CREATE TABLE statement: {statement}")
            raise RuntimeError(f"Table name transformation failed: {e}")

    def _transform_drop_table_statement(self, statement: str, database_id: int) -> str:

        # Pattern to match DROP TABLE [IF EXISTS] table_name
        pattern = r'(DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?)([\w]+)'

        def replace_table_name(match):
            prefix = match.group(1)  # "DROP TABLE IF EXISTS " or "DROP TABLE "
            table_name = match.group(2)  # table name
            new_table_name = f"{table_name}_{database_id}"
            return f"{prefix}{new_table_name}"

        transformed_statement = re.sub(pattern, replace_table_name, statement, flags=re.IGNORECASE)

        logger.debug(f"Transformed DROP: {statement.strip()[:50]}... -> {transformed_statement.strip()[:50]}...")

        return transformed_statement


    def _get_schema_file_path(self, database_type: DatabaseType) -> Path:
        """
        Get the schema file path for a specific database type.
        """
        schema_file = self.schema_dir / f"{database_type.value}_schema.sql"

        if not schema_file.exists():
            raise FileNotFoundError(
                f"Schema file not found for {database_type.value}: {schema_file}. "
                f"Please create a file named {database_type.value}_schema.sql in {self.schema_dir}"
            )

        return schema_file

    def process_schema_for_database(self, database_type, database_id: int) -> List[str]:

        schema_file = self._get_schema_file_path(database_type)
        try:
            with open(schema_file, "r") as schema:
                sql_schema = schema.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        if not sql_schema.strip():
            raise ValueError(f"Schema file {schema_file} is empty")

        lines = []
        for line in sql_schema.split("\n"):
            if '--' in line:
                line = line[:line.index('--')]
            lines.append(line)

        sql_without_comments = "\n".join(lines)

        while "/*" in sql_without_comments and "*/" in sql_without_comments:
            start = sql_without_comments.index("/*")
            end = sql_without_comments.index("/*",start) + 2
            sql_without_comments = sql_without_comments[:start] + sql_without_comments[end:]

        statements = []
        for statement in sql_without_comments.split(';'):
            cleaned_statement = statement.strip()
            if cleaned_statement:
                statements.append(cleaned_statement)

        if not statements:
            raise ValueError(f"No SQL statements found in schema file {schema_file}")

        transformed_statements = []
        for i, statement in enumerate(statements):
            try:
                if statement.strip().upper().startswith('CREATE TABLE'):
                    # Transform CREATE TABLE statements to include database ID
                    transformed_statement = self._transform_create_table_statement(statement, database_id)
                    transformed_statements.append(transformed_statement)
                elif statement.strip().upper().startswith('DROP TABLE'):
                    # Transform DROP TABLE statements to include database ID
                    transformed_statement = self._transform_drop_table_statement(statement, database_id)
                    transformed_statements.append(transformed_statement)
                    logger.debug(f"Statement {i+1}: transformed DROP TABLE")
                else:
                    # Pass through other statements unchanged (like CREATE INDEX, etc.)
                    transformed_statements.append(statement)
                    logger.debug(f"Statement {i+1}: passed through unchanged (not CREATE TABLE)")

            except Exception as e:
                logger.error(f"Failed to transform statement {i+1}: {statement[:100]}...")
                raise RuntimeError(f"Schema transformation failed on statement {i+1}: {e}")

        return transformed_statements


    async def create_schema(self, database_handler, database_type, stream_index) -> None:
        """
        Create the complete database schema for a stream configuration.
        """
        logger.info(f"Creating schema for database_id {stream_index} "
                    f"using {database_type.value} schema")

        # Get the transformed SQL statements
        sql_statements = self.process_schema_for_database(database_type, stream_index)

        # Execute each statement against the target database
        for i, statement in enumerate(sql_statements):
            try:
                logger.debug(f"Executing statement {i+1}/{len(sql_statements)}: "
                             f"{statement[:50]}{'...' if len(statement) > 50 else ''}")
                await database_handler.execute_ddl(statement)
            except Exception as e:
                logger.error(f"Failed to execute statement {i+1}: {statement}")
                raise RuntimeError(f"Schema creation failed on statement {i+1}: {e}")

        logger.info(f"Successfully created schema with {len(sql_statements)} statements")
