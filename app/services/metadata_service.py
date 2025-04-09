from collections import defaultdict

import cx_Oracle
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery
from app.models import Endpoint
import json
import hashlib
from datetime import datetime
from flask import current_app
# --- Add this import line ---
from typing import Dict, List
# --- End Add ---

# Helper function (can be outside the class or static)
def _get_oracle_dsn(endpoint):
     port = int(endpoint.port) if endpoint.port else 1521
     if endpoint.service_name:
        return cx_Oracle.makedsn(endpoint.host, port, service_name=endpoint.service_name)
     # Add elif for endpoint.sid if you store SID in your model
     else:
        raise ValueError("Oracle endpoint requires either service_name or SID.")


class MetadataService:
    @staticmethod
    def create_schema_if_not_exists(endpoint, schema_name):
        if endpoint.type == 'oracle':
            return MetadataService._create_oracle_schema(endpoint, schema_name)
        elif endpoint.type == 'mysql':
            return MetadataService._create_mysql_schema(endpoint, schema_name)
        elif endpoint.type == 'bigquery':
            return MetadataService._create_bigquery_schema(endpoint, schema_name)
        return False

    @staticmethod
    def is_table_exists( endpoint, schema, table_name):
        try:
            # Get connection strings
            conn_str = _get_connection_string(endpoint, schema)
            engine = create_engine(conn_str)

            with engine.connect() as conn:
                # Check if table exists in target
                if MetadataService._table_exists(conn, schema, table_name):
                    current_app.logger.info(f"Table {schema}.{table_name} already exists.")
                    return True
                else :
                    return False
        except Exception as e:
            current_app.logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False


    @staticmethod
    def create_tables_if_not_exists(source_endpoint, target_endpoint, source_schema, target_schema, table_name):
        try:
            # Get connection strings
            source_conn_str = _get_connection_string(source_endpoint, source_schema)
            target_conn_str = _get_connection_string(target_endpoint, target_schema)

            source_engine = create_engine(source_conn_str)
            target_engine = create_engine(target_conn_str)

            with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
                # Check if table exists in target
                if MetadataService._table_exists(target_conn, target_schema, table_name):
                    current_app.logger.info(f"Table {target_schema}.{table_name} exists. Skipping creation.")
                    return True

                # Fetch table definition from SOURCE schema
                if source_endpoint.type == 'oracle':
                    result = source_conn.execute(text(
                        f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name.upper()}', '{source_schema.upper()}') FROM DUAL"
                    ))
                    row = result.fetchone()
                    if not row:
                        raise ValueError(f"Table {source_schema}.{table_name} not found or inaccessible.")
                    table_definition = row[0]  # Use the first column of the result
                elif source_endpoint.type == 'mysql':
                    result = source_conn.execute(text(
                        f"SHOW CREATE TABLE {source_schema}.{table_name}"
                    ))
                    row = result.fetchone()
                    if not row:
                        raise ValueError(f"Table {source_schema}.{table_name} not found or inaccessible.")
                    table_definition = row[1]  # Use the second column of the result
                else:
                    raise ValueError(f"Unsupported source database: {source_endpoint.type}")

                # Modify definition to use TARGET schema
                modified_definition = table_definition.replace(
                    f'"{source_schema}"."{table_name}"',
                    f'"{target_schema}"."{table_name}"'
                )

                # Add metadata columns (ignoring table options)
                modified_definition = _add_metadata_columns(modified_definition)

                # Create the table
                target_conn.execute(text(modified_definition))
                current_app.logger.info(f"Table {target_schema}.{table_name} created successfully.")

                return True

        except Exception as e:
            current_app.logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False

    @staticmethod
    def _perform_initial_load(source_conn, target_conn, schema_name, table):
        try:
            # Get identifier columns with fallback logic
            identifier_columns, identifier_type = _get_identifier_columns(source_conn, schema_name, table)

            current_app.logger.info(
                f"Using {identifier_type} for table {schema_name}.{table}: {identifier_columns}"
            )

            # Fetch data from the source table
            data = source_conn.execute(text(f"SELECT * FROM {schema_name}.{table}")).fetchall()

            if data:
                # Get column names (case-insensitive)
                columns = [col.upper() for col in data[0].keys() if not col.upper().startswith('META_')]

                # Validate identifier columns exist in source
                missing_cols = [col for col in identifier_columns if col not in columns]
                if missing_cols:
                    raise ValueError(f"Identifier columns {missing_cols} missing in source table")

                # Insert data with metadata
                for row in data:
                    row_dict = {k.upper(): v for k, v in row._mapping.items()}

                    # Calculate hashes using identifier columns
                    meta_hash_pk = _calculate_hash([str(row_dict[col]) for col in identifier_columns])
                    meta_hash_data = _calculate_hash([str(row_dict[col]) for col in columns])

                    insert_query = text(f"""
                        INSERT INTO {schema_name}.{table} 
                        ({', '.join(columns)}, meta_hash_pk, meta_hash_data, meta_create_timestamp)
                        VALUES ({', '.join([f':{col}' for col in columns])}, 
                                :meta_hash_pk, :meta_hash_data, CURRENT_TIMESTAMP)
                    """)
                    target_conn.execute(insert_query, {
                        **row_dict,
                        'meta_hash_pk': meta_hash_pk,
                        'meta_hash_data': meta_hash_data
                    })

                current_app.logger.info(f"Initial load completed for {schema_name}.{table} ({len(data)} rows)")

            return True
        except Exception as e:
            current_app.logger.error(f"Initial load error: {str(e)}")
            return False

    @staticmethod
    def _table_exists(conn, schema_name, table_name):
        try:
            # Check if the table exists in the target database
            if conn.dialect.name == 'oracle':
                result = conn.execute(text(f"""
                    SELECT table_name 
                    FROM all_tables 
                    WHERE owner = :schema AND table_name = :table
                """), {'schema': schema_name.upper(), 'table': table_name.upper()})
            elif conn.dialect.name == 'mysql':
                result = conn.execute(text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_name = :table
                """), {'schema': schema_name, 'table': table_name})
            elif conn.dialect.name == 'postgresql':
                result = conn.execute(text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = :schema AND table_name = :table
                """), {'schema': schema_name, 'table': table_name})
                return result.fetchone() is not None
            else:
                raise ValueError(f"Unsupported database type: {conn.dialect.name}")

            return result.fetchone() is not None
        except Exception as e:
            current_app.logger.error(f"Table existence check error: {str(e)}")
            return False

    @staticmethod
    def _create_oracle_schema(endpoint, schema_name):
        try:
            dsn = f"""
            (DESCRIPTION=
                (ADDRESS=(PROTOCOL=TCP)(HOST={endpoint.host})(PORT={endpoint.port}))
                (CONNECT_DATA=(SERVICE_NAME={endpoint.service_name}))
            )"""

            engine = create_engine(
                f"oracle+cx_oracle://{endpoint.username}:{endpoint.password}@",
                connect_args={"dsn": dsn.strip().replace('\n', '')},
                max_identifier_length=128
            )
            with engine.connect() as conn:
                # Check if the schema (user) already exists
                result = conn.execute(text(f"SELECT username FROM all_users WHERE username = :schema_name"),
                                      {'schema_name': schema_name.upper()})
                if result.fetchone():
                    current_app.logger.info(f"Schema {schema_name} already exists. Skipping creation.")
                    return True

                # Create the schema (user) if it doesn't exist
                conn.execute(text(f"CREATE USER {schema_name} IDENTIFIED BY password"))
                conn.execute(text(f"GRANT CONNECT, RESOURCE TO {schema_name}"))
                current_app.logger.info(f"Schema {schema_name} created successfully.")
                return True
        except Exception as e:
            current_app.logger.error(f"Oracle schema creation error: {str(e)}")
            return False

    @staticmethod
    def _create_mysql_schema(endpoint, schema_name):
        try:
            engine = create_engine(
                f"mysql+pymysql://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/"
            )
            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema_name}"))
                return True
        except Exception as e:
            current_app.logger.error(f"MySQL schema creation error: {str(e)}")
            return False

    @staticmethod
    def _create_bigquery_schema(endpoint, schema_name):
        try:
            credentials_info = json.loads(endpoint.credentials_json)
            client = bigquery.Client.from_service_account_info(credentials_info)
            dataset = bigquery.Dataset(f"{client.project}.{schema_name}")
            dataset.location = "US"
            client.create_dataset(dataset, exists_ok=True)
            return True
        except Exception as e:
            current_app.logger.error(f"BigQuery schema creation error: {str(e)}")
            return False


    # *** ADD THIS METHOD ***
    # --- HELPER METHOD for Oracle Connection ---
    @staticmethod
    def _get_oracle_connection(endpoint: Endpoint):
        """Helper to establish an Oracle connection."""
        logger = current_app.logger # Use Flask logger
        try:
            dsn = _get_oracle_dsn(endpoint)
            logger.debug(f"Attempting Oracle connection to DSN: {dsn} for endpoint {endpoint.id}")
            conn = cx_Oracle.connect(
                user=endpoint.username,
                password=endpoint.password,
                dsn=dsn
            )
            logger.debug(f"Oracle connection successful for endpoint {endpoint.id}")
            return conn
        except cx_Oracle.Error as db_err:
            logger.error(f"Oracle connection error for endpoint {endpoint.id} ('{endpoint.name}'): {db_err}", exc_info=True)
            raise # Re-raise to be caught by calling function
        except Exception as e:
            logger.error(f"Unexpected error connecting to Oracle endpoint {endpoint.id} ('{endpoint.name}'): {e}", exc_info=True)
            raise
    # --- END HELPER METHOD ---


    @staticmethod
    def _get_oracle_schemas_and_tables(endpoint: Endpoint) -> Dict[str, List[str]]:
        """
        Fetches accessible schemas and tables for an Oracle endpoint.
        Returns a dictionary mapping schema names to lists of table names.
        """
        schemas_with_tables = defaultdict(list)
        # Define common Oracle system schemas to exclude
        excluded_schemas = {
            'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'CTXSYS',
            'DVSYS', 'EXFSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'WMSYS',
            'XDB', 'GSMADMIN_INTERNAL', 'AUDSYS', 'DBSFWUSER', 'ORDDATA',
            'ORDPLUGINS', 'SI_INFORMTN_SCHEMA', 'XS$NULL'
            # Add any other schemas you want to exclude
        }
        conn = None
        try:
            conn = MetadataService._get_oracle_connection(endpoint)
            cursor = conn.cursor()
            # Query ALL_TABLES to get tables accessible by the user
            # Filter out common system schemas and potentially temporary/nested tables
            query = """
                SELECT owner, table_name
                FROM all_tables
                WHERE owner NOT IN ({})
                  AND owner NOT LIKE 'APEX%'
                  AND nested != 'YES'
                  AND secondary != 'Y'
                ORDER BY owner, table_name
            """.format(', '.join(f"'{s}'" for s in excluded_schemas)) # Use set for faster lookup

            cursor.execute(query)
            for row in cursor:
                schema_name = row[0]
                table_name = row[1]
                schemas_with_tables[schema_name].append(table_name)

            cursor.close()
            current_app.logger.info(f"Found {sum(len(v) for v in schemas_with_tables.values())} tables across {len(schemas_with_tables)} schemas for Oracle endpoint {endpoint.id}.")
            return dict(schemas_with_tables) # Convert back to regular dict

        except cx_Oracle.Error as db_err:
            # Log specific Oracle error
            current_app.logger.error(f"Oracle metadata query error for endpoint {endpoint.id}: {db_err}", exc_info=True)
            # Return empty dict or raise exception depending on desired behavior
            return {} # Return empty on error to prevent breaking UI? Or raise
        except Exception as e:
            current_app.logger.error(f"Unexpected error fetching Oracle metadata for endpoint {endpoint.id}: {e}", exc_info=True)
            return {} # Return empty on error
        finally:
            if conn:
                try:
                    conn.close()
                    current_app.logger.debug(f"Oracle connection closed for endpoint {endpoint.id}")
                except cx_Oracle.Error:
                    pass # Ignore errors during close if connection was already bad
    # *** END ADDED METHOD ***

    @staticmethod
    def get_schemas(endpoint_data):
        try:
            if endpoint_data['type'] == 'oracle':
                return MetadataService._get_oracle_schemas(endpoint_data)
            elif endpoint_data['type'] == 'mysql':
                return MetadataService._get_mysql_schemas(endpoint_data)
            elif endpoint_data['type'] == 'bigquery':
                return MetadataService._get_bigquery_schemas(endpoint_data)
            return {}
        except Exception as e:
            current_app.logger.error(f"Metadata error: {str(e)}")
            return {}

    @staticmethod
    def _get_oracle_schemas(endpoint_data):
        current_app.logger.debug(f"Fetching schemas for Oracle endpoint: {endpoint_data}")
        try:
            dsn = cx_Oracle.makedsn(
                endpoint_data['host'],
                endpoint_data['port'],
                service_name=endpoint_data['service_name']
            )
            engine = create_engine(
                f"oracle+cx_oracle://{endpoint_data['username']}:{endpoint_data['password']}@",
                connect_args={"dsn": dsn},
                max_identifier_length=128
            )
            with engine.connect() as conn:
                schemas = {}
                # Get all schemas (excluding system users)
                result = conn.execute(text("""
                    SELECT username 
                    FROM dba_users 
                    WHERE account_status = 'OPEN'
                      AND username NOT IN ('SYS','SYSTEM','DBSNMP','XDB')
                    ORDER BY username
                """))
                for row in result:
                    schema = row[0]
                    try:
                        # Get all tables for the schema
                        tables = conn.execute(text("""
                            SELECT table_name 
                            FROM dba_tables 
                            WHERE owner = :schema
                        """), {'schema': schema}).fetchall()
                        schemas[schema] = [t[0] for t in tables]
                    except Exception as e:
                        current_app.logger.warning(f"Tables fetch error for {schema}: {str(e)}")
                        schemas[schema] = []
                return schemas
        except Exception as e:
            current_app.logger.error(f"Oracle error: {str(e)}")
            return {}

    @staticmethod
    def _get_mysql_schemas(endpoint):
        try:
            engine = create_engine(
                f"mysql+pymysql://{endpoint.username}:{endpoint.password}"
                f"@{endpoint.host}:{endpoint.port}/"
            )
            with engine.connect() as conn:
                schemas = {}
                result = conn.execute(text("SHOW DATABASES"))
                for row in result:
                    schema = row[0]
                    if schema.lower() in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                        continue
                    tables = conn.execute(text(f"SHOW TABLES FROM `{schema}`")).fetchall()
                    schemas[schema] = [t[0] for t in tables]
                return schemas
        except Exception as e:
            current_app.logger.error(f"MySQL error: {str(e)}")
            return {}

    @staticmethod
    def _get_bigquery_schemas(endpoint):
        try:
            credentials_info = json.loads(endpoint.credentials_json)
            client = bigquery.Client.from_service_account_info(credentials_info)
            schemas = {}
            for dataset in client.list_datasets():
                tables = list(client.list_tables(dataset.dataset_id))
                schemas[dataset.dataset_id] = [t.table_id for t in tables]
            return schemas
        except Exception as e:
            current_app.logger.error(f"BigQuery error: {str(e)}")
            return {}

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
