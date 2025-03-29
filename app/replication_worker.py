# replication_worker.py (Refactored)

import datetime
import time
from typing import Dict, Any, Optional

from flask import current_app  # Use Flask's logger
from sqlalchemy.exc import SQLAlchemyError

from app import db, create_app  # Assuming create_app() sets up context
from app.models import ReplicationTask, Endpoint
# --- Import Connectors and Interfaces ---
from app.interfaces import SourceConnector, TargetConnector
from .connectors.sql_alchemy_target_connector import SqlAlchemyTargetConnector
from .services.cdc.logminer import OracleLogMinerConnector
from .celery import celery_app # Assuming celery_app defined in tasks.py


# Import other connectors as you create them
# from .postgres_connector import PostgresConnector
# from .bigquery_target_connector import BigQueryTargetConnector

# --- Connector Factory Function ---
def get_source_connector(endpoint: Endpoint) -> SourceConnector:
    """Instantiates the appropriate SourceConnector based on endpoint type."""
    if endpoint.type == 'oracle':
        # Assuming OracleLogMinerConnector is the implementation for 'oracle' source
        return OracleLogMinerConnector()
    # elif endpoint.type == 'postgres':
    #     return PostgresConnector() # Add when implemented
    else:
        raise ValueError(f"Unsupported source endpoint type: {endpoint.type}")

def get_target_connector(endpoint: Endpoint) -> TargetConnector:
    """Instantiates the appropriate TargetConnector based on endpoint type."""
    # Assuming SqlAlchemyTargetConnector handles common SQL targets
    if endpoint.type in ['oracle', 'mysql', 'postgres']:
        return SqlAlchemyTargetConnector()
    # elif endpoint.type == 'bigquery':
    #     return BigQueryTargetConnector() # Add when implemented
    else:
        raise ValueError(f"Unsupported target endpoint type: {endpoint.type}")

# --- Helper to build config dict from Endpoint model ---
def build_connector_config(endpoint: Endpoint) -> Dict[str, Any]:
    """Creates a configuration dictionary for a connector from an Endpoint object."""
    config = {
        'type': endpoint.type,
        'host': endpoint.host,
        'port': endpoint.port,
        'username': endpoint.username,
        'password': endpoint.password, # Be mindful of exposing passwords in logs/errors
        'service_name': endpoint.service_name, # Oracle specific
        'database': endpoint.database,     # MySQL, Postgres specific
        'dataset': endpoint.dataset,       # BigQuery specific
        'credentials_json': endpoint.credentials_json, # BigQuery specific
        'target_schema': endpoint.target_schema, # Useful for target connector
        # Add any other relevant fields from Endpoint model
    }
    # Remove keys with None values to avoid issues in connectors
    return {k: v for k, v in config.items() if v is not None}

# --- Main Replication Function ---
@celery_app.task(bind=True) # bind=True gives access to self (the task instance)
def run_replication(self, task_id: int, perform_initial_load_override: Optional[bool] = None):
    """
    Runs the replication process for a given task ID using connectors.
    Recommendation: Run this function within a Celery/RQ task worker, not a direct thread.
    """
    app = create_app() # Create app context for db access and logging
    with app.app_context():
        task = db.session.get(ReplicationTask, task_id) # Use db.session.get for Flask-SQLAlchemy 3.x+
        if not task:
            current_app.logger.error(f"Task {task_id} not found.")
            return

        # Prevent concurrent runs (basic check, might need more robust locking)
        if task.status == 'running':
             current_app.logger.warning(f"Task {task_id} is already running. Skipping new run.")
             return

        source_endpoint = task.source
        target_endpoint = task.destination
        source_config = build_connector_config(source_endpoint)
        target_config = build_connector_config(target_endpoint)

        source_connector: Optional[SourceConnector] = None
        target_connector: Optional[TargetConnector] = None

        try:
            current_app.logger.info(f"Starting replication task '{task.name}' (ID: {task.id}).")
            task.status = 'running'
            task.last_updated = datetime.utcnow() # Add last_updated field to model if missing
            db.session.commit()

            # --- Instantiate Connectors ---
            source_connector = get_source_connector(source_endpoint)
            target_connector = get_target_connector(target_endpoint)

            # --- Connect ---
            current_app.logger.info("Connecting to source...")
            source_connector.connect(source_config)
            current_app.logger.info("Connecting to target...")
            target_connector.connect(target_config)

            # --- Initial Load (if required) ---
            # Use override if provided (e.g., for reload), otherwise use task setting
            do_initial_load = perform_initial_load_override if perform_initial_load_override is not None else task.initial_load
            if do_initial_load:
                current_app.logger.info("Starting initial load...")
                selected_tables = task.tables or [] # Expects list like [{'schema': 'HR', 'table': 'EMPLOYEES'}, ...]
                if not selected_tables:
                     current_app.logger.warning("Initial load requested but no tables specified in task configuration.")
                else:
                    for table_spec in selected_tables:
                        schema = table_spec.get('schema')
                        table = table_spec.get('table')
                        if not schema or not table:
                             current_app.logger.warning(f"Skipping invalid table specification in task: {table_spec}")
                             continue

                        current_app.logger.info(f"Processing initial load for {schema}.{table}...")

                        # Get source schema def
                        source_schema_def = source_connector.get_table_schema(schema, table)

                        # Ensure target schema exists
                        target_schema = target_config.get('target_schema', schema) # Use specified target schema or source schema
                        target_connector.create_schema_if_not_exists(target_schema)

                        # Create target table if needed (pass source definition and type)
                        if task.create_tables:
                            target_connector.create_table_if_not_exists(source_schema_def, source_type=source_endpoint.type)

                        # Transfer data in chunks
                        chunk_num = 0
                        for data_chunk in source_connector.perform_initial_load_chunk(schema, table):
                            if data_chunk:
                                target_connector.write_initial_load_chunk(target_schema, table, data_chunk)
                                chunk_num += 1
                                current_app.logger.debug(f"Wrote chunk {chunk_num} for {schema}.{table}")
                            else:
                                current_app.logger.info(f"No data found for initial load of {schema}.{table}")

                        current_app.logger.info(f"Completed initial load for {schema}.{table}.")

                # After successful initial load, update position to current source position
                current_app.logger.info("Initial load completed. Updating task position.")
                current_position = source_connector.get_current_position()
                task.last_position = current_position
                task.initial_load = False # Mark initial load as done for future runs unless overridden
                db.session.commit()
                current_app.logger.info(f"Task position updated to: {current_position}")


            # --- CDC Loop ---
            current_app.logger.info("Starting CDC loop...")
            while True:
                # Check task status before each iteration (allows external stop)
                db.session.refresh(task) # Get latest status from DB
                if task.status != 'running':
                    current_app.logger.info(f"Task status changed to '{task.status}'. Stopping CDC loop.")
                    break

                last_pos = task.last_position
                current_app.logger.debug(f"Fetching changes since position: {last_pos}")

                changes, new_pos = source_connector.get_changes(last_pos)

                if changes:
                    current_app.logger.info(f"Fetched {len(changes)} changes. Applying to target...")
                    target_connector.apply_changes(changes)

                    # --- IMPORTANT: State Update ---
                    # Update last_position *after* successfully applying changes
                    task.last_position = new_pos
                    task.last_updated = datetime.utcnow()
                    # Update metrics if needed (e.g., count inserts/updates/deletes)
                    db.session.commit()
                    current_app.logger.info(f"Changes applied. New position: {new_pos}")
                else:
                    current_app.logger.debug("No new changes detected.")
                    # Optionally update task.last_updated even if no changes
                    task.last_updated = datetime.utcnow()
                    db.session.commit()


                # Polling interval
                # TODO: Make interval configurable in task.options
                poll_interval = int(task.options.get('poll_interval_seconds', 5))
                time.sleep(poll_interval)

            # If loop exited normally (e.g., status changed), mark as stopped.
            if task.status == 'running':
                 task.status = 'stopped'

        except Exception as e:
            current_app.logger.error(f"Replication task '{task.name}' (ID: {task.id}) failed: {e}", exc_info=True) # Log traceback
            if task:
                 task.status = 'failed'
        finally:
            # --- Disconnect ---
            if source_connector:
                try:
                    current_app.logger.info("Disconnecting from source...")
                    source_connector.disconnect()
                except Exception as disc_e:
                    current_app.logger.error(f"Error disconnecting from source: {disc_e}", exc_info=True)
            if target_connector:
                try:
                    current_app.logger.info("Disconnecting from target...")
                    target_connector.disconnect()
                except Exception as disc_e:
                    current_app.logger.error(f"Error disconnecting from target: {disc_e}", exc_info=True)

            # --- Final Status Commit ---
            if task:
                try:
                    task.last_updated = datetime.utcnow()
                    db.session.commit()
                    current_app.logger.info(f"Task '{task.name}' (ID: {task.id}) finished with status: {task.status}")
                except SQLAlchemyError as commit_e:
                     current_app.logger.error(f"Failed to commit final task status: {commit_e}")
                     db.session.rollback()