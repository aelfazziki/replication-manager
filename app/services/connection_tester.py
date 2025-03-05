import cx_Oracle
from google.cloud import bigquery
import mysql.connector
import json
from sqlalchemy import create_engine


def test_database_connection(config):
    try:
        if config['type'] == 'oracle':
            dsn = f"{config['host']}:{config['port']}/{config['service_name']}"
            conn = cx_Oracle.connect(
                user=config['username'],
                password=config['password'],
                dsn=dsn
            )
            conn.ping()

        elif config['type'] == 'bigquery':
            client = bigquery.Client.from_service_account_info(
                json.loads(config['credentials_json'])
            )
            list(client.list_datasets(max_results=1))

        elif config['type'] == 'mysql':
            engine = create_engine(
                f"mysql+pymysql://{config['username']}:{config['password']}@"
                f"{config['host']}/{config['database']}"
            )
            with engine.connect() as conn:
                conn.execute("SELECT 1")

        return True, "Connexion réussie !"  # <-- Ajouter une espace après la virgule
    except Exception as e:
        return False, f"Échec de connexion : {str(e)}"