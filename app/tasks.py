# app/tasks.py (Final Version)

import time
import datetime # Use datetime directly
from typing import Dict, Any, Optional

from celery import Celery
from flask import Flask, current_app
from sqlalchemy.exc import SQLAlchemyError

# Assuming interfaces.py is in app/
from app.interfaces import SourceConnector, TargetConnector
from app import db, create_app
from app.models import ReplicationTask, Endpoint

# Import helpers using absolute path
from app.replication_worker import build_connector_config, get_source_connector, get_target_connector

# Import connectors using absolute paths (ensure these files exist at these paths)
# Wrap in try/except to allow app init even if specific connectors are missing temporarily
try:
    from app.connectors.sql_alchemy_target_connector import SqlAlchemyTargetConnector
except ImportError: SqlAlchemyTargetConnector = None; print("WARNING: SqlAlchemyTargetConnector not found")
try:
    from app.services.cdc.logminer import OracleLogMinerConnector
except ImportError: OracleLogMinerConnector = None; print("WARNING: OracleLogMinerConnector not found")
# Import other connectors here if defined

# --- Celery App Definition ---
# Relies on Flask config via init_celery for broker/backend URLs
celery_app = Celery(__name__)

def init_celery(app: Flask):
    """Initialize Celery, binding it to the Flask app context."""
    celery_app.conf.update(app.config)

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    return celery_app

# --- Task Definition ---
@celery_app.task(bind=True, ignore_result=True) # ignore_result=True if you don't need return values/status via backend often
def run_replication(self, task_id: int, perform_initial_load_override: Optional[bool] = None):
    """
    Runs the replication process for a given task ID using connectors.
    This is a Celery task.
    """
    # Check if essential connector classes imported successfully
    # Add checks for other essential connectors if you import them above
    if OracleLogMinerConnector is None or SqlAlchemyTargetConnector is None:
        # Use logger if possible, but it might not be available if app context fails
        print(f"CRITICAL ERROR: Task {task_id} cannot run: Connector classes failed to import.")
        # Optionally update task status to failed here using direct DB session if needed
        return

    app = create_app() # Create app context for db access and logging
    with app.app_context():
        # Use a separate session for the task lifecycle to avoid conflicts with Flask's request session
        # Or manage the session carefully (e.g., db.session.remove() at end)
        task = db.session.get(ReplicationTask, task_id)
        if not task:
            current_app.logger.error(f"Task {task_id} not found in run_replication.")
            return

        # Consider using a lock (e.g., Redis lock, Celery recipe) for robust concurrency control
        if task.status == 'running':
             current_app.logger.warning(f"Task {task_id} is already marked as running. Skipping new run attempt.")
             return

        source_endpoint = task.source
        target_endpoint = task.destination
        # Ensure endpoints are loaded correctly
        if not source_endpoint or not target_endpoint:
            current_app.logger.error(f"Task {task_id}: Source or Destination endpoint not found/loaded.")
            task.status = 'failed'
            db.session.commit()
            return

        source_config = build_connector_config(source_endpoint)
        target_config = build_connector_config(target_endpoint)

        source_connector: Optional[SourceConnector] = None
        target_connector: Optional[TargetConnector] = None

        try:
            current_app.logger.info(f"[Task {task_id}] Starting replication task '{task.name}'.")
            task.status = 'running'
            task.celery_task_id = self.request.id # Store celery task ID if model has field
            task.last_updated = datetime.datetime.utcnow()
            db.session.commit()

            # --- Instantiate Connectors ---
            source_connector = get_source_connector(source_endpoint)
            target_connector = get_target_connector(target_endpoint)

            # --- Connect ---
            current_app.logger.info(f"[Task {task_id}] Connecting to source: {source_endpoint.type}...")
            source_connector.connect(source_config)
            current_app.logger.info(f"[Task {task_id}] Connecting to target: {target_endpoint.type}...")
            target_connector.connect(target_config)

            # --- Initial Load ---
            do_initial_load = perform_initial_load_override if perform_initial_load_override is not None else task.initial_load
            if do_initial_load:
                # --- Initial Load Logic ---
                current_app.logger.info(f"[Task {task_id}] Starting initial load phase...")
                selected_tables = task.tables or []
                if not selected_tables:
                    current_app.logger.warning(f"[Task {task_id}] Initial load requested but no tables specified.")
                else:
                    current_app.logger.info(f"[Task {task_id}] Tables for initial load: {selected_tables}")
                    for table_spec in selected_tables:
                        schema_orig = None  # Keep original case from source list
                        table_orig = None
                        if isinstance(table_spec, dict):
                            schema_orig = table_spec.get('schema')
                            table_orig = table_spec.get('table')
                        elif isinstance(table_spec, str) and '.' in table_spec:
                            current_app.logger.warning(
                                f"[Task {task_id}] Handling table spec as 'SCHEMA.TABLE' string: {table_spec}")
                            schema_orig, table_orig = table_spec.split('.', 1)
                        else:
                            current_app.logger.warning(
                                f"[Task {task_id}] Table spec format not recognized: {table_spec}. Assuming table name only.")
                            table_orig = str(table_spec)
                            schema_orig = source_config.get('database') or source_config.get(
                                'username')  # Example default

                        if not schema_orig or not table_orig:
                            current_app.logger.warning(
                                f"[Task {task_id}] Skipping invalid/incomplete table spec after parsing: Original='{table_spec}'")
                            continue

                        # --- Convert to UPPERCASE for Oracle interaction ---
                        # Adjust this logic if dealing with non-Oracle sources/targets that are case-sensitive differently
                        schema_upper = schema_orig.upper()
                        table_upper = table_orig.upper()
                        target_schema_config = target_config.get('target_schema')
                        # Determine target schema: Use config if provided, else source schema. Uppercase if Oracle target.
                        target_schema_upper = (
                                    target_schema_config or schema_orig).upper() if target_endpoint.type == 'oracle' else (
                                    target_schema_config or schema_orig)

                        current_app.logger.info(
                            f"[Task {task_id}] Processing initial load for {schema_upper}.{table_upper} -> {target_schema_upper}.{table_upper}...")

                        try:
                            # Use original case for source interaction
                            source_schema_def = source_connector.get_table_schema(schema_orig, table_orig)
                            # Use UPPERCASE for target interaction (if target is Oracle)
                            target_connector.create_schema_if_not_exists(target_schema_upper)
                            if task.create_tables:
                                current_app.logger.info(
                                    f"[Task {task_id}] Ensuring table {target_schema_upper}.{table_upper} exists in target...")
                                # Pass original source def, connector handles conversion/creation
                                target_connector.create_table_if_not_exists(source_schema_def,
                                                                            source_type=source_endpoint.type)
                        except Exception as schema_e:
                            current_app.logger.error(
                                f"[Task {task_id}] Failed schema/table setup for {schema_upper}.{table_upper}: {schema_e}",
                                exc_info=True)
                            raise

                        chunk_num, total_rows = 0, 0
                        try:
                            # Use original case for source interaction
                            for data_chunk in source_connector.perform_initial_load_chunk(schema_orig, table_orig):
                                if data_chunk:
                                    # Use UPPERCASE for target interaction
                                    target_connector.write_initial_load_chunk(target_schema_upper, table_upper,
                                                                              data_chunk)
                                    chunk_num += 1;
                                    total_rows += len(data_chunk)
                                    current_app.logger.debug(
                                        f"[Task {task_id}] Wrote chunk {chunk_num} ({len(data_chunk)}) for {target_schema_upper}.{table_upper}")
                        except Exception as load_e:
                            current_app.logger.error(
                                f"[Task {task_id}] Failed during initial data load for {target_schema_upper}.{table_upper}: {load_e}",
                                exc_info=True)
                            raise

                        current_app.logger.info(
                            f"[Task {task_id}] Completed initial load for {target_schema_upper}.{table_upper} ({total_rows} rows).")
                    # --- End of for table_spec loop ---

                # --- Update position after ALL tables loaded successfully ---
                current_app.logger.info(f"[Task {task_id}] Initial load phase completed for all tables. Updating position.")
                try:
                    current_position = source_connector.get_current_position()
                    task.last_position = current_position
                    task.initial_load = False # Mark initial load as done
                    task.last_updated = datetime.datetime.utcnow()
                    db.session.commit()
                    current_app.logger.info(f"[Task {task_id}] Position updated to: {current_position}")
                except Exception as pos_e:
                     current_app.logger.error(f"[Task {task_id}] Failed to get/update source position after initial load: {pos_e}", exc_info=True)
                     raise # Re-raise to fail the task


            # --- CDC Loop ---
            current_app.logger.info(f"[Task {task_id}] Starting CDC loop...")
            while True:
                # Check task status from DB for external stop signal
                db.session.expire(task) # Force reload from DB on next access
                if task.status != 'running':
                    current_app.logger.info(f"[Task {task_id}] Status changed to '{task.status}'. Stopping CDC loop.")
                    break

                last_pos = task.last_position
                current_app.logger.debug(f"[Task {task_id}] Fetching changes since position: {last_pos}")

                try:
                    changes, new_pos = source_connector.get_changes(last_pos)
                except Exception as fetch_e:
                    current_app.logger.error(f"[Task {task_id}] Failed to fetch changes: {fetch_e}", exc_info=True)
                    # Decide: Break loop and fail task, or sleep and retry? Let's break for now.
                    task.status = 'failed'
                    break # Exit the while loop

                if changes:
                    current_app.logger.info(f"[Task {task_id}] Fetched {len(changes)} changes. Applying...")
                    try:
                        target_connector.apply_changes(changes)
                        # --- IMPORTANT: State Update ---
                        task.last_position = new_pos # Update position *after* successful apply
                        task.last_updated = datetime.datetime.utcnow()
                        db.session.commit() # Commit position update
                        current_app.logger.info(f"[Task {task_id}] Changes applied. New position: {new_pos}")
                    except Exception as apply_e:
                        current_app.logger.error(f"[Task {task_id}] Failed to apply changes: {apply_e}", exc_info=True)
                        db.session.rollback() # Rollback any partial apply if target TX failed
                        # Decide: Break loop and fail task, or log and continue? Let's break for now.
                        task.status = 'failed'
                        break # Exit the while loop
                else:
                    current_app.logger.debug(f"[Task {task_id}] No new changes detected.")
                    task.last_updated = datetime.datetime.utcnow() # Update timestamp even if no changes
                    db.session.commit()

                # Polling interval
                poll_interval = 10 # Default, consider making configurable via task.options
                try:
                     poll_interval = int(task.options.get('poll_interval_seconds', 10))
                except (ValueError, TypeError):
                     poll_interval = 10 # Fallback to default
                current_app.logger.debug(f"[Task {task_id}] Sleeping for {poll_interval} seconds...")
                time.sleep(poll_interval)
            # --- End CDC Loop ---

            # If loop exited cleanly (status change check), mark as stopped
            if task.status == 'running':
                 task.status = 'stopped'

        except Exception as e:
            # Catch errors during setup (connect, initial load) or unhandled errors in loop
            current_app.logger.error(f"[Task {task_id}] Replication failed: {e}", exc_info=True)
            if task and task.status != 'failed': # Check if already marked as failed inside loop
                 task.status = 'failed'
                 task.last_updated = datetime.datetime.utcnow()
                 db.session.commit() # Commit status change on error
        finally:
            # --- Disconnect ---
            current_app.logger.info(f"[Task {task_id}] Disconnecting connectors...")
            if source_connector:
                try: source_connector.disconnect()
                except Exception as disc_e: current_app.logger.error(f"[Task {task_id}] Error disconnecting source: {disc_e}", exc_info=True)
            if target_connector:
                try: target_connector.disconnect()
                except Exception as disc_e: current_app.logger.error(f"[Task {task_id}] Error disconnecting target: {disc_e}", exc_info=True)

            # --- Final Status Commit ---
            if task:
                try:
                    # Ensure final status is committed if changed
                    if db.session.is_modified(task):
                        task.last_updated = datetime.datetime.utcnow()
                        db.session.commit()
                    current_app.logger.info(f"[Task {task.id}] Worker function finished. Final DB status: {task.status}")
                except SQLAlchemyError as commit_e:
                     current_app.logger.error(f"[Task {task.id}] Failed to commit final task status: {commit_e}")
                     db.session.rollback()