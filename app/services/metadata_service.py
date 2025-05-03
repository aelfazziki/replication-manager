from collections import defaultdict

import oracledb as cx_Oracle
from sqlalchemy import create_engine, inspect, text
#from sqlalchemy.dialects.postgresql import psycopg2
import psycopg2  # Add this import at the top of the file
from psycopg2 import OperationalError  # Specific error for connection issues
from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery

from app.models import Endpoint
import json
import hashlib
from datetime import datetime
from flask import current_app
# --- Add this import line ---
from typing import Dict, List, Tuple,Any,Optional


# --- End Add ---

# Helper function (can be outside the class or static)
def _get_oracle_dsn(endpoint):
     port = int(endpoint.port) if endpoint.port else 1521
     if endpoint.service_name:
        return cx_Oracle.makedsn(endpoint.host, port, service_name=endpoint.service_name)
     # Add elif for endpoint.sid if you store SID in your model
     else:
        raise ValueError("Oracle endpoint requires either service_name or SID.")


# --- MetadataService Class ---
class MetadataService:

    # === Connection Helper (Oracle Specific) ===
    @staticmethod
    def _get_oracle_connection(endpoint: Endpoint) -> cx_Oracle.Connection:
        """Helper to establish a direct cx_Oracle connection."""
        conn = None
        endpoint_id_log = endpoint.id if endpoint.id else 'TEMP'
        try:
            dsn = _get_oracle_dsn(endpoint) # Raises ValueError if service_name missing
            current_app.logger.debug(f"Attempting Oracle connection to DSN: {dsn} for endpoint ID: {endpoint_id_log}")
            # Consider initializing Oracle client if needed
            # cx_Oracle.init_oracle_client(lib_dir=r"C:\path\to\your\instantclient")
            conn = cx_Oracle.connect(
                user=endpoint.username,
                password=endpoint.password,
                dsn=dsn
            )
            current_app.logger.debug(f"Oracle connection successful for endpoint ID: {endpoint_id_log}")
            return conn
        except cx_Oracle.Error as db_err:
            current_app.logger.error(f"Oracle connection error for endpoint ID: {endpoint_id_log} ('{endpoint.name}'): {db_err}", exc_info=True)
            raise # Re-raise to be caught by calling function
        except Exception as e:
            current_app.logger.error(f"Unexpected error connecting to Oracle endpoint ID: {endpoint_id_log} ('{endpoint.name}'): {e}", exc_info=True)
            raise

    @staticmethod
    def _get_postgres_connection_string(endpoint, schema_name=None):
        """Generate PostgreSQL connection string."""
        conn_params = {
            'host': endpoint.host,
            'port': endpoint.port,
            'dbname': endpoint.database,
            'user': endpoint.username
        }

        if endpoint.password:
            conn_params['password'] = endpoint.password

        if schema_name:
            conn_params['options'] = f'-c search_path={schema_name}'

        # Convert to connection string format
        return ' '.join([f"{k}='{v}'" for k, v in conn_params.items()])

    # === Connection Testing ===
    @staticmethod
    def test_connection(endpoint: Endpoint) -> Tuple[bool, str]:
        """
        Attempts to establish a connection based on endpoint details.
        Returns: A tuple: (success: bool, message: str)
        """
        endpoint_id_log = endpoint.id if endpoint.id else 'TEMP'
        current_app.logger.info(f"Testing connection for endpoint: '{endpoint.name}' (ID: {endpoint_id_log}), type: {endpoint.type}, host: {endpoint.host}")
        if endpoint.type == 'postgres':
            # For PostgreSQL, schema_name is optional for connection testing
            conn_str = MetadataService._get_postgres_connection_string(endpoint, schema_name=None)
            current_app.logger.info(f"Testing PostgreSQL connection with: {conn_str}")

            # Try to connect
            conn = None
            try:
                conn = psycopg2.connect(conn_str)
                if conn:
                    conn.close()
                    return True, "PostgreSQL connection successful"
            except OperationalError as e:
                return False, f"PostgreSQL connection failed: {str(e)}"
            except Exception as e:
                return False, f"PostgreSQL connection error: {str(e)}"

        elif endpoint.type == 'oracle':
            conn = None
            try:
                conn = MetadataService._get_oracle_connection(endpoint)
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL") # Simple query
                cursor.fetchone()
                cursor.close()
                current_app.logger.info(f"Oracle connection test successful for endpoint '{endpoint.name}' (ID: {endpoint_id_log})")
                return True, "Oracle connection successful!"
            except Exception as e:
                current_app.logger.error(f"Oracle connection test failed for endpoint '{endpoint.name}' (ID: {endpoint_id_log}): {e}", exc_info=True)
                error_message = str(e)
                if hasattr(e, 'args') and e.args and isinstance(e.args[0], cx_Oracle.Error):
                     error_obj, = e.args
                     error_message = f"ORA-{error_obj.code:05d}: {error_obj.message}"
                return False, f"Oracle connection failed: {error_message}"
            finally:
                if conn:
                    try: conn.close()
                    except Exception: pass

        elif endpoint.type == 'postgres':
            conn_str = None
            try:
                conn_str = _get_connection_string(endpoint) # Use helper
                engine = create_engine(conn_str, connect_args={'connect_timeout': 5})
                with engine.connect() as connection:
                    connection.execute(text("SELECT 1"))
                current_app.logger.info(f"PostgreSQL connection test successful for endpoint '{endpoint.name}' (ID: {endpoint_id_log})")
                return True, "PostgreSQL connection successful!"
            except SQLAlchemyError as e:
                 current_app.logger.error(f"PostgreSQL connection test failed (SQLAlchemyError) for endpoint '{endpoint.name}' (ID: {endpoint_id_log}, ConnStr: {conn_str}): {e}", exc_info=True)
                 return False, f"PostgreSQL connection failed: {e}"
            except Exception as e:
                 current_app.logger.error(f"PostgreSQL connection test failed (Other Error) for endpoint '{endpoint.name}' (ID: {endpoint_id_log}, ConnStr: {conn_str}): {e}", exc_info=True)
                 return False, f"PostgreSQL connection failed: {e}"

        # Add elif blocks for 'mysql', 'bigquery', etc. here if needed
        # elif endpoint.type == 'mysql': ...
        # elif endpoint.type == 'bigquery': ...

        else:
            current_app.logger.warning(f"Connection test not implemented for type: {endpoint.type} (Endpoint '{endpoint.name}', ID: {endpoint_id_log})")
            return False, f"Connection test not implemented for type: {endpoint.type}"

    # === Schema/Table Metadata Fetching ===
    # Renamed _get_postgres_schemas to match Oracle naming convention
    @staticmethod
    def _get_postgres_schemas_and_tables(endpoint: Endpoint) -> Dict[str, List[str]]:
        """ Fetches accessible schemas and tables for Postgres """
        schemas = defaultdict(list)
        excluded_schemas = ('pg_catalog', 'information_schema', 'pg_toast')
        conn_str = None
        endpoint_id_log = endpoint.id if endpoint.id else 'TEMP'
        current_app.logger.info(f"Fetching Postgres schemas/tables for endpoint {endpoint_id_log}")
        try:
            conn_str = _get_connection_string(endpoint) # Use helper
            engine = create_engine(conn_str, connect_args={'connect_timeout': 10}) # Increased timeout slightly
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE' AND table_schema NOT IN :excluded
                    ORDER BY table_schema, table_name
                """), {"excluded": excluded_schemas})
                fetched_rows = 0
                for row in result:
                    fetched_rows += 1
                    schemas[row[0]].append(row[1]) # schema=row[0], table=row[1]
            current_app.logger.info(f"Found {sum(len(v) for v in schemas.values())} tables ({fetched_rows} rows read) across {len(schemas)} schemas for Postgres endpoint {endpoint_id_log}.")
            return dict(schemas)
        except Exception as e:
             current_app.logger.error(f"Error fetching Postgres metadata for endpoint {endpoint_id_log} (ConnStr: {conn_str}): {e}", exc_info=True)
             return {}

    @staticmethod
    def _get_oracle_schemas_and_tables(endpoint: Endpoint) -> Dict[str, List[str]]:
        """ Fetches accessible schemas and tables for an Oracle endpoint. """
        schemas_with_tables = defaultdict(list)
        excluded_schemas = { 'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'CTXSYS', 'DVSYS', 'EXFSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'WMSYS', 'XDB', 'GSMADMIN_INTERNAL', 'AUDSYS', 'DBSFWUSER', 'ORDDATA', 'ORDPLUGINS', 'SI_INFORMTN_SCHEMA', 'XS$NULL' }
        conn = None
        endpoint_id_log = endpoint.id if endpoint.id else 'TEMP'
        current_app.logger.info(f"Fetching Oracle schemas/tables for endpoint {endpoint_id_log}")
        try:
            conn = MetadataService._get_oracle_connection(endpoint)
            cursor = conn.cursor()
            query = """
                SELECT owner, table_name FROM all_tables
                WHERE owner NOT IN ({}) AND owner NOT LIKE 'APEX%'
                  AND nested != 'YES' AND secondary != 'Y'
                ORDER BY owner, table_name
            """.format(', '.join(f"'{s}'" for s in excluded_schemas))
            current_app.logger.debug(f"Executing Oracle metadata query for endpoint {endpoint_id_log}")
            cursor.execute(query)
            fetched_rows = 0
            for row in cursor:
                fetched_rows += 1
                schemas_with_tables[row[0]].append(row[1])
            cursor.close()
            current_app.logger.info(f"Found {sum(len(v) for v in schemas_with_tables.values())} tables ({fetched_rows} rows read) across {len(schemas_with_tables)} schemas for Oracle endpoint {endpoint_id_log}.")
            return dict(schemas_with_tables)
        except Exception as e:
            current_app.logger.error(f"Error fetching Oracle metadata for endpoint {endpoint_id_log}: {e}", exc_info=True)
            return {}
        finally:
            if conn:
                try: conn.close(); current_app.logger.debug(f"Oracle connection closed for endpoint {endpoint_id_log}")
                except Exception: pass

    # Add _get_mysql_schemas_and_tables if needed

    @staticmethod
    def get_table_columns(endpoint: Endpoint, schema_name: str, table_name: str) -> List[Dict[str,Any]]:
        """ Fetches column names and types for a specific table using SQLAlchemy inspect. """
        endpoint_id_log = endpoint.id if endpoint.id else 'TEMP'
        current_app.logger.info(f"Fetching columns for {schema_name}.{table_name} on endpoint {endpoint_id_log} (type: {endpoint.type})")
        columns = []
        engine = None # Define engine outside try
        try:
            conn_str = _get_connection_string(endpoint, schema_name)
            engine = create_engine(conn_str)
            inspector = inspect(engine)
            reflected_columns = inspector.get_columns(table_name, schema=schema_name)

            # Handle Oracle case sensitivity if needed
            if not reflected_columns and endpoint.type == 'oracle':
                 current_app.logger.debug(f"No columns found for {schema_name}.{table_name}, trying uppercase...")
                 reflected_columns = inspector.get_columns(table_name.upper(), schema=schema_name.upper())

            if not reflected_columns:
                 current_app.logger.warning(f"Could not find columns for table {schema_name}.{table_name} using inspector.")
                 return [] # Return empty list if table/columns not found by inspector

            for col in reflected_columns:
                 columns.append({
                     'name': col['name'],
                     'type': str(col['type']),
                     'nullable': col['nullable']
                     # 'pk': col.get('primary_key', False) # Add if needed
                 })
            current_app.logger.info(f"Found {len(columns)} columns for {schema_name}.{table_name} on endpoint {endpoint_id_log}")
            return columns
        except Exception as e:
            current_app.logger.error(f"Error fetching columns for {schema_name}.{table_name} on endpoint {endpoint_id_log}: {e}", exc_info=True)
            return []
        finally:
             if engine: # Dispose engine if created
                 engine.dispose()


    # === Schema/Table Creation/Existence ===
    # These methods seem specific and might be better placed elsewhere or refactored
    # Keeping them for now based on user's provided code, ensure they are static

    @staticmethod
    def create_schema_if_not_exists(endpoint, schema_name):
        """Creates schema if it doesn't exist (dialect specific)."""
        current_app.logger.info(f"Checking/Creating schema '{schema_name}' for endpoint {endpoint.id} (type: {endpoint.type})")
        if endpoint.type == 'oracle':
            # Note: Creating Oracle schemas (users) usually requires high privileges
            # It's often better done manually by a DBA.
            return MetadataService._create_oracle_schema(endpoint, schema_name)
        elif endpoint.type == 'postgres':
             return MetadataService._create_postgres_schema(endpoint, schema_name)
        # Add MySQL, BigQuery etc.
        # elif endpoint.type == 'mysql': ...
        else:
            current_app.logger.error(f"Schema creation not implemented for type: {endpoint.type}")
            return False

    @staticmethod
    def _create_oracle_schema(endpoint, schema_name):
        """Creates an Oracle User (Schema). Needs high privileges."""
        # Consider removing this method and requiring manual schema creation for Oracle
        conn = None
        try:
            conn = MetadataService._get_oracle_connection(endpoint) # Assumes connecting user has CREATE USER privs
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM all_users WHERE username = :uname", uname=schema_name.upper())
            if cursor.fetchone():
                current_app.logger.info(f"Oracle schema (user) '{schema_name}' already exists.")
                return True
            else:
                current_app.logger.info(f"Attempting to create Oracle schema (user) '{schema_name}'...")
                default_password = "ChangeMe!" # Extremely insecure default
                cursor.execute(f"CREATE USER {schema_name} IDENTIFIED BY \"{default_password}\"")
                cursor.execute(f"GRANT CONNECT, RESOURCE TO {schema_name}")
                cursor.execute(f"ALTER USER {schema_name} QUOTA UNLIMITED ON USERS")
                conn.commit()
                current_app.logger.info(f"Oracle schema (user) '{schema_name}' created successfully (with insecure default password!).")
                return True
        except Exception as e:
            current_app.logger.error(f"Oracle schema creation error for '{schema_name}': {e}", exc_info=True)
            return False
        finally:
            if conn:
                try: conn.close()
                except Exception: pass

    @staticmethod
    def _create_postgres_schema(endpoint, schema_name):
        """Creates a PostgreSQL schema."""
        conn_str = None
        engine = None
        try:
            conn_str = _get_connection_string(endpoint)
            engine = create_engine(conn_str)
            with engine.connect() as conn:
                 # Check if schema exists first
                 schema_exists_query = text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema")
                 result = conn.execute(schema_exists_query, {'schema': schema_name})
                 if result.fetchone():
                     current_app.logger.info(f"PostgreSQL schema '{schema_name}' already exists.")
                     return True
                 else:
                     current_app.logger.info(f"Creating PostgreSQL schema '{schema_name}'...")
                     conn.execute(text(f"CREATE SCHEMA \"{schema_name}\"")) # Use quotes for safety
                     conn.commit()
                     current_app.logger.info(f"PostgreSQL schema '{schema_name}' created.")
                     return True
        except Exception as e:
            current_app.logger.error(f"PostgreSQL schema creation error for '{schema_name}' (ConnStr: {conn_str}): {e}", exc_info=True)
            # Rollback might be needed if connection was implicitly transactional
            return False
        finally:
             if engine: engine.dispose()


    # --- Removed other potentially duplicate/unused methods from user's previous paste ---
    # Such as: is_table_exists, create_tables_if_not_exists, _perform_initial_load,
    # get_schemas, _get_oracle_schemas, _get_mysql_schemas, _get_bigquery_schemas,
    # _add_metadata_columns, _get_primary_keys, _get_identifier_columns etc.
    # These functionalities seem better handled by the connectors and task logic directly.
    # If any are definitely needed, they should be reviewed and potentially refactored.

# --- End MetadataService Class ---


def _get_connection_string(endpoint, schema_name):
    if endpoint.type == 'oracle':
        return f"oracle+cx_oracle://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/?service_name={endpoint.service_name}"
    elif endpoint.type == 'mysql':
        return f"mysql+pymysql://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{schema_name}"
    elif endpoint.type == 'bigquery':
        return f"bigquery://{schema_name}"
    elif endpoint.type == 'postgres':
        return f"postgresql+psycopg2://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{endpoint.database}?options=-csearch_path%3D{schema_name}"
    else:
        raise ValueError(f"Unsupported database type: {endpoint.type}")

def _add_metadata_columns(table_definition):
    """
    Append metadata columns to the column definitions and ignore table options.
    """
    # Find the opening parenthesis of the column list
    start_pos = table_definition.find("(")
    if start_pos == -1:
        return table_definition  # Invalid DDL; return as-is

    # Track parentheses to find the matching closing parenthesis for the column list
    open_parentheses = 1
    closing_pos = start_pos + 1
    while closing_pos < len(table_definition) and open_parentheses > 0:
        if table_definition[closing_pos] == "(":
            open_parentheses += 1
        elif table_definition[closing_pos] == ")":
            open_parentheses -= 1
        closing_pos += 1

    if open_parentheses != 0:
        return table_definition  # Mismatched parentheses; return as-is

    # Extract the column definitions (excluding the final closing parenthesis)
    column_defs = table_definition[: closing_pos - 1].rstrip()

    # Append metadata columns and close the column list
    metadata_columns = """
    , meta_hash_pk VARCHAR2(64)
    , meta_hash_data VARCHAR2(64)
    , meta_create_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    , meta_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )"""
    modified_definition = column_defs + metadata_columns

    return modified_definition

@staticmethod
def _perform_initial_load(source_conn, target_conn, schema_name, table):
    try:
        # Fetch primary keys for the table
        primary_keys = _get_primary_keys(source_conn, schema_name, table)

        # Fetch data from the source table
        data = source_conn.execute(text(f"SELECT * FROM {schema_name}.{table}")).fetchall()

        if data:
            # Get column names
            columns = data[0].keys()

            # Insert data into the target table with metadata columns
            for row in data:
                # Calculate meta_hash_pk and meta_hash_data
                meta_hash_pk = _calculate_hash([str(row[col]) for col in primary_keys])
                meta_hash_data = _calculate_hash([str(row[col]) for col in columns])

                # Prepare the insert query
                insert_query = f"""
                INSERT INTO {schema_name}.{table} ({', '.join(columns)}, meta_hash_pk, meta_hash_data, meta_create_timestamp, meta_update_timestamp)
                VALUES ({', '.join([':' + col for col in columns])}, :meta_hash_pk, :meta_hash_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                target_conn.execute(text(insert_query), {**row, 'meta_hash_pk': meta_hash_pk, 'meta_hash_data': meta_hash_data})

        return True
    except Exception as e:
        current_app.logger.error(f"Initial load error: {str(e)}")
        return False


def _get_primary_keys(conn, schema_name, table_name):
    try:
        # Oracle-specific primary key detection (uppercase normalized)
        query = text("""
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols 
            ON cons.constraint_name = cols.constraint_name
            AND cons.owner = cols.owner
            WHERE cons.owner = :schema
            AND cons.table_name = :table
            AND cons.constraint_type = 'P'
            ORDER BY cols.position
        """)
        result = conn.execute(query,
                              {'schema': schema_name.upper(),
                               'table': table_name.upper()})
        primary_keys = [row[0].upper() for row in result]  # Force uppercase

        if not primary_keys:
            current_app.logger.warning(f"No primary key found for {schema_name}.{table_name}")
            # Fallback to all columns
            result = conn.execute(text("""
                SELECT column_name 
                FROM all_tab_columns 
                WHERE owner = :schema 
                AND table_name = :table
            """), {'schema': schema_name.upper(), 'table': table_name.upper()})
            primary_keys = [row[0].upper() for row in result]  # Force uppercase

        return primary_keys

    except Exception as e:
        current_app.logger.error(f"Primary key fetch error: {str(e)}")
        return []

def _calculate_hash(values):
    # Calculate a hash of the given values
    return hashlib.sha256(''.join(values).encode()).hexdigest()


def perform_initial_load(source_endpoint, target_endpoint, source_schema, target_schema, table_name):
    try:
        metrics = {
            'inserts': 0,
            'bytes_processed': 0
        }

        source_conn_str = _get_connection_string(source_endpoint, source_schema)
        target_conn_str = _get_connection_string(target_endpoint, target_schema)

        source_engine = create_engine(source_conn_str)
        target_engine = create_engine(target_conn_str)

        with source_engine.connect() as source_conn, \
                target_engine.connect() as target_conn:

            # Get identifier columns using fallback logic
            identifier_columns, identifier_type = _get_identifier_columns(
                source_conn, source_schema, table_name
            )

            if not identifier_columns:
                raise ValueError(f"No identifiable columns found for {source_schema}.{table_name}")

            current_app.logger.info(
                f"Using {identifier_type} columns for {source_schema}.{table_name}: {identifier_columns}"
            )

            # Get primary keys (uppercase)
            primary_keys = [pk.upper() for pk in _get_primary_keys(source_conn, source_schema, table_name)]

            if not primary_keys:
                raise ValueError(f"No columns found for {source_schema}.{table_name}")

            # Fetch data and get column names (uppercase)
            result = source_conn.execute(
                text(f"SELECT * FROM {source_schema}.{table_name}")
            )
            columns = [col.upper() for col in result.keys() if not col.upper().startswith('META_')]

            # Verify primary keys exist in columns
            missing_pks = [pk for pk in primary_keys if pk not in columns]
            if missing_pks:
                raise ValueError(f"Primary keys {missing_pks} missing in source table")

            # Prepare insert statement
            insert_stmt = text(f"""
                INSERT INTO {target_schema}.{table_name} 
                ({', '.join(columns)}, meta_hash_pk, meta_hash_data, meta_create_timestamp)
                VALUES ({', '.join([f':{col}' for col in columns])}, 
                        :meta_hash_pk, :meta_hash_data, CURRENT_TIMESTAMP)
            """)

            # Batch processing with case-insensitive access
            batch_size = 1000
            while True:
                batch = result.fetchmany(batch_size)
                if not batch:
                    break

                # Calculate batch metrics
                batch_bytes = sum(len(str(row).encode('utf-8')) for row in batch)
                metrics['inserts'] += len(batch)
                metrics['bytes_processed'] += batch_bytes

                params_list = []
                for row in batch:
                    row_dict = {k.upper(): v for k, v in row._mapping.items()}  # Case-insensitive access

                    # Calculate hashes using uppercase keys
                    meta_hash_pk = _calculate_hash([str(row_dict[pk]) for pk in primary_keys])
                    meta_hash_data = _calculate_hash([str(row_dict[col]) for col in columns])

                    params = {col: row_dict[col] for col in columns}
                    params.update({
                        'meta_hash_pk': meta_hash_pk,
                        'meta_hash_data': meta_hash_data
                    })
                    params_list.append(params)

                target_conn.execute(insert_stmt, params_list)
                target_conn.commit()

                current_app.logger.debug(
                    f"Processed batch - "
                    f"Rows: {len(batch)}, "
                    f"Bytes: {batch_bytes}"
                )

            current_app.logger.info(
                f"Initial load completed - "
                f"Total rows: {metrics['inserts']}, "
                f"Total data: {metrics['bytes_processed']} bytes"
            )
            return metrics

    except Exception as e:
        current_app.logger.error(f"Initial load failed for {table_name}: {str(e)}", exc_info=True)
        return False

def _get_primary_key_columns(conn, schema_name, table_name):
    # Existing PK detection logic
    query = text("""
        SELECT cols.column_name
        FROM all_constraints cons
        JOIN all_cons_columns cols 
        ON cons.constraint_name = cols.constraint_name
        AND cons.owner = cols.owner
        WHERE cons.owner = :schema
        AND cons.table_name = :table
        AND cons.constraint_type = 'P'
        ORDER BY cols.position
    """)
    result = conn.execute(query, {'schema': schema_name.upper(), 'table': table_name.upper()})
    return [row[0].upper() for row in result]


def _get_identifier_columns(conn, schema_name, table_name):
    """Get best available identifier columns (PK > Unique Index > All Columns)"""
    try:
        # 1. Try Primary Key
        pk_cols = _get_primary_key_columns(conn, schema_name, table_name)
        if pk_cols:
            return pk_cols, "primary key"

        # 2. Try Unique Indexes
        unique_cols = _get_unique_index_columns(conn, schema_name, table_name)
        if unique_cols:
            return unique_cols, "unique index"

        # 3. Fallback to All Columns
        all_cols = _get_all_columns(conn, schema_name, table_name)
        if not all_cols:
            raise ValueError(f"No columns found for {schema_name}.{table_name}")
        return all_cols, "all columns"

    except Exception as e:
        current_app.logger.error(f"Identifier detection error: {str(e)}")
        return [], "none"


def _get_unique_index_columns(conn, schema_name, table_name):
    """Get columns from first non-system unique index"""
    try:
        query = text("""
            SELECT cols.column_name, cols.column_position
            FROM all_indexes idx
            JOIN all_ind_columns cols
            ON idx.index_name = cols.index_name
            AND idx.table_owner = cols.table_owner
            WHERE idx.table_owner = :schema
            AND idx.table_name = :table
            AND idx.uniqueness = 'UNIQUE'
            AND idx.index_name NOT LIKE 'SYS_%'
            ORDER BY cols.index_name, cols.column_position
        """)
        result = conn.execute(query, {
            'schema': schema_name.upper(),
            'table': table_name.upper()
        })

        # Group columns by index
        index_map = defaultdict(list)
        for row in result:
            index_map[row[0]].append(row[1].upper())  # Store column positions by index name

        # Return columns from first valid index
        return next(iter(index_map.values()), [])

    except Exception as e:
        current_app.logger.error(f"Unique index detection error: {str(e)}")
        return []

def _get_all_columns(conn, schema_name, table_name):
    """Get all table columns"""
    query = text("""
        SELECT column_name 
        FROM all_tab_columns 
        WHERE owner = :schema 
        AND table_name = :table
        ORDER BY column_id
    """)
    result = conn.execute(query, {'schema': schema_name.upper(), 'table': table_name.upper()})
    return [row[0].upper() for row in result]

    @staticmethod
    def _create_postgres_schema(endpoint, schema_name):
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{endpoint.database}"
            )
            with engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                return True
        except Exception as e:
            current_app.logger.error(f"PostgreSQL schema creation error: {str(e)}")
            return False

    @staticmethod
    def _get_postgres_schemas(endpoint):
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{endpoint.database}"
            )
            with engine.connect() as conn:
                schemas = {}
                result = conn.execute(text("SELECT schema_name FROM information_schema.schemata"))
                for row in result:
                    schema = row[0]
                    tables = conn.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")).fetchall()
                    schemas[schema] = [t[0] for t in tables]
                return schemas
        except Exception as e:
            current_app.logger.error(f"PostgreSQL error: {str(e)}")
            return {}
    # *** ADD THIS STATIC METHOD ***



    # --- Keep other existing static methods ---
    # _get_oracle_schemas_and_tables, _get_postgres_schemas, etc.
    # ...

# --- End MetadataService Class ---
#**Explanation:**
#1.  **`@staticmethod`:** Defines `test_connection` as a static method, so you can call it directly on the class (`MetadataService.test_connection(...)`) as done in `routes.py`.
#2.  **Input:** Takes an `Endpoint` object (which `routes.py` creates temporarily from the form data).
#3.  **Type Handling:** Uses `if/elif/else` to check `endpoint.type`.
#4.  **Oracle:** Reuses the `_get_oracle_connection` helper method. If it returns successfully without raising an exception, the connection worked. It ensures the connection is closed in a `finally` block.
#5.  **PostgreSQL:** Builds a SQLAlchemy connection string and uses `create_engine().connect()` within a `try...except` block. It executes a simple `SELECT 1` to verify. Includes a short connection timeout.
#6.  **Other Types:** Includes commented-out placeholders for MySQL and BigQuery as examples. You would need to add similar connection logic for any other database types you support.
#7.  **Return Value:** Returns a tuple `(boolean_success, string_message)`.
#**After applying this fix:**
#
#1.  Save the changes to `app/services/metadata_service.py`.
#2.  Restart your Flask application (`flask run`).
#3.  Go to the "Create Endpoint" or "Edit Endpoint" page in the UI.
#4.  Fill in the details for your `CDB$ROOT` connection.
#5.  Click the "Test Connection" button.
