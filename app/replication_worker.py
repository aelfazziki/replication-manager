# app/replication_worker.py (Final Version)

from typing import Dict, Any
from app.models import Endpoint

# --- Import Connectors using Absolute Paths ---
# Assuming standard project structure: app/connectors/, app/services/cdc/
from app.interfaces import SourceConnector, TargetConnector
try:
    from app.connectors.sql_alchemy_target_connector import SqlAlchemyTargetConnector
except ImportError: SqlAlchemyTargetConnector = None; print("WARNING: SqlAlchemyTargetConnector not found in replication_worker.py")
try:
    from app.services.cdc.logminer import OracleLogMinerConnector
except ImportError: OracleLogMinerConnector = None; print("WARNING: OracleLogMinerConnector not found in replication_worker.py")
# Import other connectors here if defined

# --- Connector Factory Function ---
def get_source_connector(endpoint: Endpoint) -> SourceConnector:
    """Instantiates the appropriate SourceConnector based on endpoint type."""
    if endpoint.type == 'oracle':
        if OracleLogMinerConnector is None:
             raise ImportError("OracleLogMinerConnector class not available.")
        return OracleLogMinerConnector()
    # elif endpoint.type == 'postgres':
    #     if PostgresConnector is None: raise ImportError(...)
    #     return PostgresConnector()
    else:
        raise ValueError(f"Unsupported source endpoint type: {endpoint.type}")

def get_target_connector(endpoint: Endpoint) -> TargetConnector:
    """Instantiates the appropriate TargetConnector based on endpoint type."""
    if endpoint.type in ['oracle', 'mysql', 'postgres']:
        if SqlAlchemyTargetConnector is None:
            raise ImportError("SqlAlchemyTargetConnector class not available.")
        return SqlAlchemyTargetConnector()
    # elif endpoint.type == 'bigquery':
    #     if BigQueryTargetConnector is None: raise ImportError(...)
    #     return BigQueryTargetConnector()
    else:
        raise ValueError(f"Unsupported target endpoint type: {endpoint.type}")

# --- Helper to build config dict from Endpoint model ---
def build_connector_config(endpoint: Endpoint) -> Dict[str, Any]:
    """Creates a configuration dictionary for a connector from an Endpoint object."""
    # Provide default empty strings for optional fields if None to simplify downstream checks
    config = {
        'type': endpoint.type,
        'host': endpoint.host,
        'port': endpoint.port,
        'username': endpoint.username,
        'password': endpoint.password, # Consider encryption/decryption here
        'service_name': endpoint.service_name or '',
        'database': endpoint.database or '',
        'dataset': endpoint.dataset or '',
        'credentials_json': endpoint.credentials_json or '',
        'target_schema': endpoint.target_schema or '',
    }
    # Return only keys with actual values (or customize based on connector needs)
    return {k: v for k, v in config.items() if v is not None}