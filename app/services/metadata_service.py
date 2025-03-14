from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery
from app.models import Endpoint
import json
import hashlib
from datetime import datetime
from flask import current_app


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

    def _perform_initial_load(source_conn, target_conn, schema_name, table):
        try:
            # Fetch primary keys for the table
            current_app.logger.info(f"Fetching primary keys for table {schema_name}.{table}.")
            primary_keys = _get_primary_keys(source_conn, schema_name, table)

            # Fetch data from the source table
            current_app.logger.info(f"Fetching data from source table {schema_name}.{table}.")
            data = source_conn.execute(text(f"SELECT * FROM {schema_name}.{table}")).fetchall()

            if data:
                # Get column names
                columns = data[0].keys()

                # Insert data into the target table with metadata columns
                current_app.logger.info(f"Inserting data into target table {schema_name}.{table}.")
                for row in data:
                    # Calculate meta_hash_pk and meta_hash_data
                    meta_hash_pk = _calculate_hash([str(row[col]) for col in primary_keys])
                    meta_hash_data = _calculate_hash([str(row[col]) for col in columns])

                    # Prepare the insert query
                    insert_query = f"""
                    INSERT INTO {schema_name}.{table} ({', '.join(columns)}, meta_hash_pk, meta_hash_data, meta_create_timestamp, meta_update_timestamp)
                    VALUES ({', '.join([':' + col for col in columns])}, :meta_hash_pk, :meta_hash_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                    target_conn.execute(text(insert_query),
                                        {**row, 'meta_hash_pk': meta_hash_pk, 'meta_hash_data': meta_hash_data})

                current_app.logger.info(f"Initial load for table {schema_name}.{table} completed successfully.")
            else:
                current_app.logger.info(f"No data found in source table {schema_name}.{table}.")
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
        try:
            dsn = f"""
            (DESCRIPTION=
                (ADDRESS=(PROTOCOL=TCP)(HOST={endpoint_data['host']})(PORT={endpoint_data['port']}))
                (CONNECT_DATA=(SERVICE_NAME={endpoint_data['service_name']}))
            )"""

            engine = create_engine(
                f"oracle+cx_oracle://{endpoint_data['username']}:{endpoint_data['password']}@",
                connect_args={"dsn": dsn.strip().replace('\n', '')},
                max_identifier_length=128
            )
            with engine.connect() as conn:
                schemas = {}
                result = conn.execute(text("SELECT username FROM all_users ORDER BY username"))
                for row in result:
                    schema = row[0]
                    try:
                        tables = conn.execute(text(f"""
                            SELECT table_name 
                            FROM all_tables 
                            WHERE owner = :schema
                        """), {'schema': schema}).fetchall()
                        schemas[schema] = [t[0] for t in tables]
                    except:
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
        # Use Easy Connect syntax: host:port/service_name
        return f"oracle+cx_oracle://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/?service_name={endpoint.service_name}"
    elif endpoint.type == 'mysql':
        return f"mysql+pymysql://{endpoint.username}:{endpoint.password}@{endpoint.host}:{endpoint.port}/{schema_name}"
    elif endpoint.type == 'bigquery':
        return f"bigquery://{schema_name}"
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


def _get_primary_keys(conn, schema_name, table):
    try:
        # Fetch primary key columns for the table
        result = conn.execute(text(f"""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_schema = :schema AND table_name = :table AND constraint_name = 'PRIMARY'
        """), {'schema': schema_name, 'table': table})
        return [row[0] for row in result]
    except Exception as e:
        current_app.logger.error(f"Primary key fetch error: {str(e)}")
        return []


def _calculate_hash(values):
    # Calculate a hash of the given values
    return hashlib.sha256(''.join(values).encode()).hexdigest()


class MetadataService:
    @staticmethod
    def perform_initial_load(source_endpoint, target_endpoint, source_schema, target_schema, table_name):
        try:
            # Get connections
            source_conn_str = _get_connection_string(source_endpoint, source_schema)
            target_conn_str = _get_connection_string(target_endpoint, target_schema)

            source_engine = create_engine(source_conn_str)
            target_engine = create_engine(target_conn_str)

            with source_engine.connect() as source_conn, \
                    target_engine.connect() as target_conn:

                # Get primary keys for hash calculation
                primary_keys = _get_primary_keys(source_conn, source_schema, table_name)

                # Fetch data from source
                result = source_conn.execute(text(f"SELECT * FROM {source_schema}.{table_name}"))
                rows = result.fetchall()

                if rows:
                    # Get column names (excluding metadata columns)
                    columns = [col for col in rows[0].keys() if not col.startswith('meta_')]
                    col_names = ', '.join(columns)
                    placeholders = ', '.join([f":{col}" for col in columns])

                    # Prepare insert statement
                    insert_stmt = f"""
                        INSERT INTO {target_schema}.{table_name} 
                        ({col_names}, meta_hash_pk, meta_hash_data, meta_create_timestamp)
                        VALUES ({placeholders}, :meta_hash_pk, :meta_hash_data, CURRENT_TIMESTAMP)
                    """

                    # Insert data in batches
                    batch_size = 1000
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i + batch_size]
                        for row in batch:
                            # Calculate hashes
                            meta_hash_pk = _calculate_hash([str(row[col]) for col in primary_keys])
                            meta_hash_data = _calculate_hash([str(row[col]) for col in columns])

                            # Prepare parameters
                            params = {col: row[col] for col in columns}
                            params.update({
                                'meta_hash_pk': meta_hash_pk,
                                'meta_hash_data': meta_hash_data
                            })

                            target_conn.execute(text(insert_stmt), params)

                        target_conn.commit()

                    current_app.logger.info(f"Initial load completed for {table_name} ({len(rows)} rows)")

                return True

        except Exception as e:
            current_app.logger.error(f"Initial load failed for {table_name}: {str(e)}")
            return False