from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery
from flask import current_app
from app.models import Endpoint
import json


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
                conn.execute(text(f"CREATE USER {schema_name} IDENTIFIED BY password"))
                conn.execute(text(f"GRANT CONNECT, RESOURCE TO {schema_name}"))
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
            return False    @staticmethod
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