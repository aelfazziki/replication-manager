# sql_alchemy_target_connector.py (or add to interfaces.py)

from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column # Added Column
#from sqlalchemy.dialects.oracle import oracledb
import oracledb
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
# --- Add imports for expression language ---
from sqlalchemy.sql import insert, update, delete # Removed bindparam as not used yet
from typing import Any, Dict, List, Optional

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import TargetConnector
# --- Import the schema converter ---
from .schema_converter import BasicSqlAlchemyConverter # Adjust import if needed
from flask import current_app

class SqlAlchemyTargetConnector(TargetConnector):
    """
    TargetConnector implementation using SQLAlchemy Core.
    Aims to support multiple standard SQL databases (Oracle, MySQL, Postgres, etc.).
    """

    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.engine: Optional[Engine] = None
        self.connection: Optional[Connection] = None
        self.metadata: MetaData = MetaData()
        # --- Instantiate the schema converter ---
        self.schema_converter: BasicSqlAlchemyConverter = BasicSqlAlchemyConverter()

    def _get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string from config."""
        db_type = self.config.get('type')
        user = self.config.get('username')
        password = self.config.get('password')
        host = self.config.get('host')
        port = self.config.get('port')

        if db_type == 'oracle':
            # Expects 'service_name' in config
            service_name = self.config.get('service_name')
            if not service_name:
                raise ValueError("Oracle target requires 'service_name' in config.")
            # Ensure cx_Oracle is installed: pip install sqlalchemy cx_Oracle
#            return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}"
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
        elif db_type == 'mysql':
            # Expects 'database' in config
            database = self.config.get('database')
            if not database:
                raise ValueError("MySQL target requires 'database' in config.")
            # Ensure PyMySQL is installed: pip install sqlalchemy pymysql
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        elif db_type == 'postgres':
            # Expects 'database' in config
            database = self.config.get('database')
            if not database:
                raise ValueError("PostgreSQL target requires 'database' in config.")
            # Ensure psycopg2 is installed: pip install sqlalchemy psycopg2-binary
            return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        # Add other dialects (e.g., sqlserver) here
        else:
            raise ValueError(f"Unsupported database type for SQLAlchemy target: {db_type}")

    def connect(self, config: Dict[str, Any]) -> None:
        """Establish connection using SQLAlchemy."""
        if self.engine:
            self.disconnect()

        self.config = config
        conn_str = self._get_connection_string()
        try:
            # Add this line before creating engines (only needed if using thick mode)
            #oracledb.init_oracle_client()

            # Consider engine options like pool_size, max_overflow, etc. for production
            self.engine = create_engine(conn_str)
            # Establish a connection to test validity
            self.connection = self.engine.connect()
            current_app.logger.info(f"Successfully connected to {self.config.get('type')} target.") # Replace with logging
        except SQLAlchemyError as e:
            current_app.logger.info(f"Error connecting to {self.config.get('type')} target: {e}") # Replace with logging
            self.engine = None
            self.connection = None
            raise

    def disconnect(self) -> None:
        """Dispose engine and close connection."""
        if self.connection:
            try:
                self.connection.close()
            except SQLAlchemyError as e:
                current_app.logger.error(f"Error closing SQLAlchemy connection: {e}") # Replace with logging
            finally:
                self.connection = None
        if self.engine:
            try:
                self.engine.dispose()
            except SQLAlchemyError as e:
                current_app.logger.error(f"Error disposing SQLAlchemy engine: {e}") # Replace with logging
            finally:
                self.engine = None
        self.config = {}
        current_app.logger.info("Disconnected from SQLAlchemy target.") # Replace with logging

    def _get_table(self, schema_name: str, table_name: str) -> Table:
        """Gets SQLAlchemy Table object, using reflection and caching in self.metadata."""
        # Construct a unique key for the metadata cache
        # Handle None schema for databases that don't use it explicitly (like default SQLite)
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name

        if full_table_name in self.metadata.tables:
            return self.metadata.tables[full_table_name]
        else:
            if not self.engine:
                 raise ConnectionError("Engine not available for table reflection.")
            try:
                 # Bind metadata to the engine allows reflection results to be cached
                 #if self.metadata.bind is None:
                 #     self.metadata.bind = self.engine
                 # Reflect table structure from the database into self.metadata
                 # autoload_with requires the engine or connection
                 reflected_table = Table(
                     table_name,
                     self.metadata, # Use self.metadata for caching
                     autoload_with=self.engine,
                     schema=schema_name
                 )
                 current_app.logger.debug(f"Reflected target table structure for {full_table_name}")
                 return reflected_table
            except NoSuchTableError:
                 current_app.logger.error(f"Table {full_table_name} not found in target database during reflection for apply_changes.")
                 raise # Re-raise the error


    def apply_changes(self, changes: List[Dict[str, Any]]) -> None:
        """
        Apply a batch of *structured* change events using SQLAlchemy Expression Language.
        """
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")

        try:
            with self.connection.begin(): # Use a transaction
                for change in changes:
                    operation = change.get('operation', '').lower()
                    schema = change.get('schema')
                    table_name = change.get('table')
                    primary_keys_data = change.get('primary_keys', {}) # Dict of {'pk_col': value}
                    before_data = change.get('before_data', {})
                    after_data = change.get('after_data', {})

                    if not schema or not table_name:
                        current_app.logger.info(f"Skipping change - Missing schema or table name: {change}") # Replace with logging
                        continue

                    try:
                        target_table = self._get_table(schema, table_name)
                    except (NoSuchTableError, SQLAlchemyError) as e:
                         current_app.logger.info(f"Skipping change - Cannot get table structure for {schema}.{table_name}: {e}") # Replace with logging
                         continue # Or handle error differently (e.g., fail the batch)

                    if operation == 'insert':
                        if not after_data:
                             current_app.logger.info(f"Skipping insert - Missing 'after_data': {change}") # Replace with logging
                             continue
                        stmt = insert(target_table).values(after_data)
                        self.connection.execute(stmt)

                    elif operation == 'delete':
                        if not primary_keys_data:
                             current_app.logger.info(f"Skipping delete - Missing 'primary_keys' data: {change}") # Replace with logging
                             continue
                        stmt = delete(target_table)
                        # Build WHERE clause based on primary keys
                        for pk_col, pk_val in primary_keys_data.items():
                            if pk_col in target_table.c:
                                stmt = stmt.where(target_table.c[pk_col] == pk_val)
                            else:
                                current_app.logger.info(f"Warning: PK column '{pk_col}' not found in target table '{target_table.fullname}' for DELETE.") # Replace with logging
                        self.connection.execute(stmt)

                    elif operation == 'update':
                        if not primary_keys_data or not after_data:
                             current_app.logger.info(f"Skipping update - Missing 'primary_keys' or 'after_data': {change}") # Replace with logging
                             continue
                        stmt = update(target_table)
                        # Build WHERE clause based on primary keys
                        for pk_col, pk_val in primary_keys_data.items():
                            if pk_col in target_table.c:
                                stmt = stmt.where(target_table.c[pk_col] == pk_val)
                            else:
                                current_app.logger.warning(f"Warning: PK column '{pk_col}' not found in target table '{target_table.fullname}' for UPDATE WHERE.") # Replace with logging
                        # Set values from after_data (excluding PKs if they shouldn't be updated)
                        values_to_update = {k: v for k, v in after_data.items() if k not in primary_keys_data and k in target_table.c}
                        if not values_to_update:
                             current_app.logger.info(f"Skipping update - No non-PK columns found in 'after_data' to update for: {change}") # Replace with logging
                             continue
                        stmt = stmt.values(values_to_update)
                        self.connection.execute(stmt)

                    else:
                        current_app.logger.info(f"Skipping change - Unsupported operation '{operation}': {change}") # Replace with logging

            current_app.logger.info(f"Successfully applied {len(changes)} structured changes.") # Replace with logging
        except SQLAlchemyError as e:
            current_app.logger.error(f"Error applying structured changes: {e}") # Replace with logging
            # Transaction rolls back automatically
            raise

    # ... (create_schema_if_not_exists, create_table_if_not_exists, write_initial_load_chunk remain the same for now) ...
    def create_schema_if_not_exists(self, schema_name: str) -> None:
        """Create the schema in the target if it doesn't already exist."""
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")

        inspector = inspect(self.engine)
        try:
            # SQLAlchemy's inspector doesn't have a simple has_schema, check schemas list
            existing_schemas = inspector.get_schema_names()
            if schema_name.lower() in [s.lower() for s in existing_schemas]:
                 current_app.logger.info(f"Schema '{schema_name}' already exists.") # Replace with logging
                 return

            # CREATE SCHEMA syntax varies slightly (e.g., AUTHORIZATION in some dialects)
            # Using simple CREATE SCHEMA for broad compatibility, might need adjustments.
            # Ensure schema_name is properly sanitized/quoted if necessary.
            current_app.logger.info(f"Attempting to create schema '{schema_name}'...") # Replace with logging
            with self.connection.begin(): # Use transaction
                 self.connection.execute(text(f'CREATE SCHEMA "{schema_name}"')) # Basic quoting
            current_app.logger.info(f"Schema '{schema_name}' created or already exists.") # Replace with logging

        except SQLAlchemyError as e:
            # Handle errors, e.g., permissions, dialect incompatibility
            current_app.logger.error(f"Error creating schema '{schema_name}': {e}") # Replace with logging
            # Check if error indicates it already exists (might happen due to race conditions or specific DB behavior)
            # If not a "schema already exists" error, re-raise.
            raise

    def create_table_if_not_exists(self, source_table_definition: Dict[str, Any], source_type: str) -> None:
        """
        Create the table in the target based on a standardized definition if it doesn't exist.
        Uses the SchemaConverter for type mapping and SQLAlchemy schema tools for creation.
        *** Corrected to use target schema/table names. ***
        Args:
            source_table_definition: Standardized table definition from the SourceConnector.
            source_type: String identifier for the source DB type (e.g., 'oracle').
        """
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")

        source_schema_name = source_table_definition.get('schema')
        source_table_name = source_table_definition.get('table')
        target_db_type = self.config.get('type')

        if not source_schema_name or not source_table_name:
             raise ValueError("Source table definition must include 'schema' and 'table' names.")
        if not target_db_type:
             raise ValueError("Target connector configuration must include 'type'.")
        if not hasattr(self, 'schema_converter'):
             raise AttributeError("Schema converter not initialized on target connector.")

        # --- Determine TARGET schema and table names ---
        target_schema_name = self.config.get('target_schema', source_schema_name) # Use target_schema from config or default to source
        target_table_name = source_table_name # Usually target table name is same as source

        # --- Apply Case Correction for Target (e.g., Oracle) ---
        if target_db_type == 'oracle':
            target_schema_name = target_schema_name.upper()
            target_table_name = target_table_name.upper()
        # Add elif for other case-sensitive targets if needed

        current_app.logger.info(f"Checking existence of target table: '{target_schema_name}'.'{target_table_name}'")

        inspector = inspect(self.engine)
        try:
            # --- Use TARGET names for check ---
            if inspector.has_table(target_table_name, schema=target_schema_name):
                # --- Corrected Log Message ---
                current_app.logger.warning(f"Table '{target_schema_name}'.'{target_table_name}' already exists in target.")
                return
            else:
                current_app.logger.info(f"Table '{target_schema_name}'.'{target_table_name}' does not exist. Attempting creation...")

                # --- 1. Convert Schema using source def and types ---
                target_table_def = self.schema_converter.convert_schema(
                    source_table_definition,
                    source_type,
                    target_db_type
                )

                # --- 2. Build SQLAlchemy Table Object using TARGET names ---
                sqlalchemy_columns = []
                for col_def in target_table_def.get('columns', []):
                    # Ensure type is included, handle potential errors during conversion
                    if 'type' not in col_def or col_def['type'] is None:
                         raise ValueError(f"Schema conversion failed for column '{col_def.get('name')}': No type information.")
                    col = Column(
                        col_def['name'],
                        col_def['type'], # SQLAlchemy Type instance from converter
                        primary_key=col_def.get('primary_key', False),
                        nullable=col_def.get('nullable', True)
                    )
                    sqlalchemy_columns.append(col)

                if not sqlalchemy_columns:
                     raise ValueError(f"No columns defined after conversion for table {target_schema_name}.{target_table_name}")

                # Bind metadata to engine for reflection/creation context
                # Using temporary metadata for create_all avoids conflicts if reflecting later
                temp_metadata = MetaData()
                sqlalchemy_table = Table(
                    target_table_name, # Use TARGET name
                    temp_metadata,
                    *sqlalchemy_columns,
                    schema=target_schema_name # Use TARGET schema
                )

                # --- 3. Create Table in Database ---
                current_app.logger.info(f"Executing CREATE TABLE for '{target_schema_name}'.'{target_table_name}'...")
                with self.connection.begin(): # Use transaction
                    # create_all will issue CREATE TABLE statement
                    temp_metadata.create_all(self.engine, tables=[sqlalchemy_table], checkfirst=False)

                current_app.logger.info(f"Successfully created table '{target_schema_name}'.'{target_table_name}'.")

        except SQLAlchemyError as e:
            current_app.logger.error(f"Error checking/creating table '{target_schema_name}'.'{target_table_name}': {e}", exc_info=True)
            raise
        except Exception as e: # Catch other potential errors (e.g., conversion)
             current_app.logger.error(f"Unexpected error during table creation for '{target_schema_name}'.'{target_table_name}': {e}", exc_info=True)
             raise
    from sqlalchemy import Table, MetaData, Column  # Ensure Column is imported
    # *** ADD THIS METHOD IMPLEMENTATION ***
    # *** REPLACE existing truncate_table method with this ***
    def truncate_table(self, schema_name: str, table_name: str) -> None:
        """Deletes all rows from the specified table using direct DELETE statement."""
        if not self.connection or not self.engine: # Need engine for inspect
            raise ConnectionError("Target connection or engine not established.")

        logger = current_app.logger
        logger.info(f"Attempting to clear data from target table '{schema_name}'.'{table_name}'...")

        inspector = inspect(self.engine)
        has_table = False
        try:
            # Check table existence using inspector, handling case sensitivity for Oracle
            # Oracle typically stores unquoted identifiers in uppercase.
            # Adjust schema/table names if needed based on how they are stored/queried.
            has_table = inspector.has_table(table_name, schema=schema_name)
            # Optionally, try uppercase if case sensitivity is the issue:
            # if not has_table:
            #     has_table = inspector.has_table(table_name.upper(), schema=schema_name.upper())
            #     if has_table:
            #         table_name = table_name.upper()
            #         schema_name = schema_name.upper()

        except Exception as inspect_err:
             logger.warning(f"Error checking existence for table '{schema_name}'.'{table_name}': {inspect_err}. Assuming it might exist and proceeding with DELETE.", exc_info=True)
             # If inspect fails, we can still try the DELETE and let it fail if table truly doesn't exist
             has_table = True # Proceed cautiously

        if not has_table:
             logger.warning(f"Inspector confirmed table '{schema_name}'.'{table_name}' not found in target, skipping clear.")
             return # Explicitly return if table not found

        # If table exists (or check failed), attempt DELETE
        try:
            # Use standard SQL DELETE FROM statement via text() for broader compatibility
            # Ensure schema/table names are quoted correctly for the specific database dialect (Oracle uses double quotes)
            delete_sql = text(f'DELETE FROM "{schema_name}"."{table_name}"')

            with self.connection.begin(): # Use transaction
                result = self.connection.execute(delete_sql)

            # Log success (rowcount might be -1 or unreliable)
            logger.info(f"Successfully executed clear command for '{schema_name}'.'{table_name}'.")

        except SQLAlchemyError as e:
            logger.error(f"Error clearing data from '{schema_name}'.'{table_name}': {e}", exc_info=True)
            raise # Re-raise to signal failure in the task
        except Exception as e:
            logger.error(f"Unexpected error clearing data from '{schema_name}'.'{table_name}': {e}", exc_info=True)
            raise
    # *** END REPLACE ***

    def write_initial_load_chunk(self, schema_name: str, table_name: str, data_chunk: List[Dict[str, Any]]) -> None:
        """
        Write a chunk of data during initial load using SQLAlchemy Core execution.
        Builds a minimal Table object based on data keys instead of autoloading.
        """
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")
        if not data_chunk:
            return

        # --- Build Minimal Table Object ---
        # Use column names from the first row of the data chunk.
        # Assumes all chunks have the same columns and data is Dict-like.
        first_row = data_chunk[0]
        column_names = list(first_row.keys())

        # Create SQLAlchemy Column objects minimally (type can often be omitted for INSERT)
        sqlalchemy_columns = [Column(name) for name in column_names]

        # Create a Table object bound to this operation's metadata
        # Note: This Table object only exists for this insert operation.
        # It doesn't use autoload and only knows about columns in the data chunk.
        target_table = Table(
            table_name,
            MetaData(), # Use temporary metadata, not self.metadata to avoid conflicts
            *sqlalchemy_columns,
            schema=schema_name
        )
        # --- End Build Minimal Table Object ---

        try:
            # Perform bulk insert using the constructed Table object
            with self.connection.begin(): # Use transaction
                # SQLAlchemy's insert construct will build statement using columns
                # from the Table object and values from the data_chunk list of dicts.
                self.connection.execute(target_table.insert(), data_chunk)

            current_app.logger.info(f"Inserted {len(data_chunk)} rows into '{schema_name}'.'{table_name}'.") # Changed log level to debug

        except SQLAlchemyError as e:
            # Log specific error and potentially the chunk size/keys for debugging
            current_app.logger.error(
                f"Error writing initial load chunk to '{schema_name}'.'{table_name}': {e}. "
                f"Chunk size: {len(data_chunk)}, Columns: {column_names}"
                , exc_info=True # Include traceback in log
            )
            raise # Re-raise the exception to signal failure in the task
        except Exception as e: # Catch other potential errors
            current_app.logger.error(
                f"Unexpected error writing initial load chunk to '{schema_name}'.'{table_name}': {e}. "
                f"Chunk size: {len(data_chunk)}, Columns: {column_names}"
                , exc_info=True
            )
            raise