# sql_alchemy_target_connector.py (or add to interfaces.py)

from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column # Added Column
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
# --- Add imports for expression language ---
from sqlalchemy.sql import insert, update, delete # Removed bindparam as not used yet
from typing import Any, Dict, List, Optional

# Assuming interfaces.py is in the same directory or adjust import path
from app.interfaces import TargetConnector
# --- Import the schema converter ---
from .schema_converter import BasicSqlAlchemyConverter # Adjust import if needed

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
            return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service_name}"
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
            # Consider engine options like pool_size, max_overflow, etc. for production
            self.engine = create_engine(conn_str)
            # Establish a connection to test validity
            self.connection = self.engine.connect()
            print(f"Successfully connected to {self.config.get('type')} target.") # Replace with logging
        except SQLAlchemyError as e:
            print(f"Error connecting to {self.config.get('type')} target: {e}") # Replace with logging
            self.engine = None
            self.connection = None
            raise

    def disconnect(self) -> None:
        """Dispose engine and close connection."""
        if self.connection:
            try:
                self.connection.close()
            except SQLAlchemyError as e:
                print(f"Error closing SQLAlchemy connection: {e}") # Replace with logging
            finally:
                self.connection = None
        if self.engine:
            try:
                self.engine.dispose()
            except SQLAlchemyError as e:
                print(f"Error disposing SQLAlchemy engine: {e}") # Replace with logging
            finally:
                self.engine = None
        self.config = {}
        print("Disconnected from SQLAlchemy target.") # Replace with logging

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
                        print(f"Skipping change - Missing schema or table name: {change}") # Replace with logging
                        continue

                    try:
                        target_table = self._get_table(schema, table_name)
                    except (NoSuchTableError, SQLAlchemyError) as e:
                         print(f"Skipping change - Cannot get table structure for {schema}.{table_name}: {e}") # Replace with logging
                         continue # Or handle error differently (e.g., fail the batch)

                    if operation == 'insert':
                        if not after_data:
                             print(f"Skipping insert - Missing 'after_data': {change}") # Replace with logging
                             continue
                        stmt = insert(target_table).values(after_data)
                        self.connection.execute(stmt)

                    elif operation == 'delete':
                        if not primary_keys_data:
                             print(f"Skipping delete - Missing 'primary_keys' data: {change}") # Replace with logging
                             continue
                        stmt = delete(target_table)
                        # Build WHERE clause based on primary keys
                        for pk_col, pk_val in primary_keys_data.items():
                            if pk_col in target_table.c:
                                stmt = stmt.where(target_table.c[pk_col] == pk_val)
                            else:
                                print(f"Warning: PK column '{pk_col}' not found in target table '{target_table.fullname}' for DELETE.") # Replace with logging
                        self.connection.execute(stmt)

                    elif operation == 'update':
                        if not primary_keys_data or not after_data:
                             print(f"Skipping update - Missing 'primary_keys' or 'after_data': {change}") # Replace with logging
                             continue
                        stmt = update(target_table)
                        # Build WHERE clause based on primary keys
                        for pk_col, pk_val in primary_keys_data.items():
                            if pk_col in target_table.c:
                                stmt = stmt.where(target_table.c[pk_col] == pk_val)
                            else:
                                print(f"Warning: PK column '{pk_col}' not found in target table '{target_table.fullname}' for UPDATE WHERE.") # Replace with logging
                        # Set values from after_data (excluding PKs if they shouldn't be updated)
                        values_to_update = {k: v for k, v in after_data.items() if k not in primary_keys_data and k in target_table.c}
                        if not values_to_update:
                             print(f"Skipping update - No non-PK columns found in 'after_data' to update for: {change}") # Replace with logging
                             continue
                        stmt = stmt.values(values_to_update)
                        self.connection.execute(stmt)

                    else:
                        print(f"Skipping change - Unsupported operation '{operation}': {change}") # Replace with logging

            print(f"Successfully applied {len(changes)} structured changes.") # Replace with logging
        except SQLAlchemyError as e:
            print(f"Error applying structured changes: {e}") # Replace with logging
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
                 print(f"Schema '{schema_name}' already exists.") # Replace with logging
                 return

            # CREATE SCHEMA syntax varies slightly (e.g., AUTHORIZATION in some dialects)
            # Using simple CREATE SCHEMA for broad compatibility, might need adjustments.
            # Ensure schema_name is properly sanitized/quoted if necessary.
            print(f"Attempting to create schema '{schema_name}'...") # Replace with logging
            with self.connection.begin(): # Use transaction
                 self.connection.execute(text(f'CREATE SCHEMA "{schema_name}"')) # Basic quoting
            print(f"Schema '{schema_name}' created or already exists.") # Replace with logging

        except SQLAlchemyError as e:
            # Handle errors, e.g., permissions, dialect incompatibility
            print(f"Error creating schema '{schema_name}': {e}") # Replace with logging
            # Check if error indicates it already exists (might happen due to race conditions or specific DB behavior)
            # If not a "schema already exists" error, re-raise.
            raise

    def create_table_if_not_exists(self, source_table_definition: Dict[str, Any], source_type: str) -> None:
        """
        Create the table in the target based on a standardized definition if it doesn't exist.
        Uses the SchemaConverter for type mapping and SQLAlchemy schema tools for creation.

        Args:
            source_table_definition: Standardized table definition from the SourceConnector.
            source_type: String identifier for the source DB type (e.g., 'oracle').
                         Needed by the converter.
        """
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")

        schema_name = source_table_definition.get('schema')
        table_name = source_table_definition.get('table')
        target_type = self.config.get('type') # Get target type from config

        if not schema_name or not table_name:
             raise ValueError("Source table definition must include 'schema' and 'table' names.")
        if not target_type:
             raise ValueError("Target connector configuration must include 'type'.")

        inspector = inspect(self.engine)
        try:
            if inspector.has_table(table_name, schema=schema_name):
                print(f"Table '{schema_name}'.'{table_name}' already exists in target.") # Replace with logging
                return
            else:
                print(f"Table '{schema_name}'.'{table_name}' does not exist. Attempting creation...") # Replace with logging

                # --- 1. Convert Schema ---
                target_table_def = self.schema_converter.convert_schema(
                    source_table_definition,
                    source_type,
                    target_type
                )

                # --- 2. Build SQLAlchemy Table Object ---
                sqlalchemy_columns = []
                for col_def in target_table_def.get('columns', []):
                    col = Column(
                        col_def['name'],
                        col_def['type'], # This is now a SQLAlchemy Type instance
                        primary_key=col_def.get('primary_key', False),
                        nullable=col_def.get('nullable', True)
                        # Add server_default, etc. if needed
                    )
                    sqlalchemy_columns.append(col)

                if not sqlalchemy_columns:
                     raise ValueError(f"No columns defined after conversion for table {schema_name}.{table_name}")

                # Bind metadata to engine for reflection/creation context
                self.metadata.bind = self.engine
                sqlalchemy_table = Table(
                    table_name,
                    self.metadata,
                    *sqlalchemy_columns, # Unpack the list of Column objects
                    schema=schema_name
                )

                # --- 3. Create Table in Database ---
                # Use transaction for table creation
                with self.connection.begin():
                    # create_all checks for existence by default, but we already checked.
                    # Using checkfirst=False avoids a redundant check.
                    self.metadata.create_all(self.engine, tables=[sqlalchemy_table], checkfirst=False)

                print(f"Successfully created table '{schema_name}'.'{table_name}'.") # Replace with logging

        except SQLAlchemyError as e:
            print(f"Error checking/creating table '{schema_name}'.'{table_name}': {e}") # Replace with logging
            # Consider specific error handling (e.g., permissions, invalid types)
            raise
        except Exception as e: # Catch potential errors during conversion or object creation
             print(f"Unexpected error during table creation for '{schema_name}'.'{table_name}': {e}") # Replace with logging
             raise

    def write_initial_load_chunk(self, schema_name: str, table_name: str, data_chunk: List[Dict[str, Any]]) -> None:
        """Write a chunk of data during initial load using SQLAlchemy bulk insert."""
        if not self.connection or not self.engine:
            raise ConnectionError("Not connected to target database.")
        if not data_chunk:
            return

        try:
            # Reflect table structure (or ideally, have it from create_table)
            # Binding metadata to the engine allows reflection
            self.metadata.bind = self.engine
            target_table = Table(table_name, self.metadata, autoload_with=self.engine, schema=schema_name)

            # Perform bulk insert
            with self.connection.begin(): # Use transaction
                self.connection.execute(target_table.insert(), data_chunk)
            print(f"Inserted {len(data_chunk)} rows into '{schema_name}'.'{table_name}'.") # Replace with logging
        except NoSuchTableError:
             print(f"Error writing initial load: Table '{schema_name}'.'{table_name}' not found. Ensure it was created first.") # Replace with logging
             raise
        except SQLAlchemyError as e:
            print(f"Error writing initial load chunk to '{schema_name}'.'{table_name}': {e}") # Replace with logging
            # Consider logging problematic data from data_chunk if possible
            raise