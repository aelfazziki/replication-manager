from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery
from flask import current_app
from app.models import Endpoint
import json


class MetadataService:
    @staticmethod
    def get_schemas(endpoint_id):
        try:
            endpoint = Endpoint.query.get(endpoint_id)
            if not endpoint:
                return {}

            if endpoint.type == 'oracle':
                return MetadataService._get_oracle_schemas(endpoint)
            elif endpoint.type == 'mysql':
                return MetadataService._get_mysql_schemas(endpoint)
            elif endpoint.type == 'bigquery':
                return MetadataService._get_bigquery_schemas(endpoint)

            return {}

        except Exception as e:
            current_app.logger.error(f"Metadata error: {str(e)}")
            return {}

    @staticmethod
    def _get_oracle_schemas(endpoint):
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
                # Get all schemas (users)
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