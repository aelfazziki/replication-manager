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
from redis import Redis # Import Redis client

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
@celery_app.task(bind=True) # Add bind=True to access self.request.id
def run_replication(self, task_id: int, start_datetime: Optional[str] = None):
    """
    Celery task to perform database replication for a given task ID.
    Includes initial load and Change Data Capture (CDC).
    Supports graceful shutdown via Redis flag.
    """
    task: Optional[ReplicationTask] = None
    source_endpoint: Optional[Endpoint] = None
    target_endpoint: Optional[Endpoint] = None
    source_connector: Optional[SourceConnector] = None
    target_connector: Optional[TargetConnector] = None
    stop_requested = False # Flag to track if stop was initiated
    redis_client = None # Initialize Redis client
    stop_key = None # Initialize Redis stop key name

    try:
        # --- Connect to Redis for stop signal checking ---
        try:
            # Use the Celery result backend URL for Redis connection
            redis_url = current_app.config.get('CELERY_RESULT_BACKEND')
            if redis_url and redis_url.startswith('redis://'):
                 redis_client = Redis.from_url(redis_url)
                 # Use the unique Celery task ID for the stop key
                 stop_key = f"stop_request:{self.request.id}"
                 # Clear any potential stale flag from previous runs of this task ID
                 redis_client.delete(stop_key)
                 current_app.logger.info(f"[Task {task_id}] Connected to Redis. Stop key: '{stop_key}'")
            else:
                 current_app.logger.warning(f"[Task {task_id}] Redis backend not configured or invalid URL in CELERY_RESULT_BACKEND. Stop signal check disabled.")
        except Exception as redis_e:
             current_app.logger.warning(f"[Task {task_id}] Could not connect to Redis for stop signal check: {redis_e}", exc_info=True)
             redis_client = None # Disable Redis check if connection fails

        # --- Fetch Task and Endpoints ---
        current_app.logger.info(f"[Task {task_id}] Starting replication task.")
        task = db.session.get(ReplicationTask, task_id)
        if not task:
            current_app.logger.error(f"[Task {task_id}] ReplicationTask not found in database.")
            return # Or raise an error

        # Check if task is already in a terminal state or stopping
        if task.status in ['stopped', 'failed', 'completed', 'stopping']:
             current_app.logger.warning(f"[Task {task_id}] Task is already in status '{task.status}'. Aborting run.")
             return

        # Update task status to 'running'
        task.status = 'running'
        task.celery_task_id = self.request.id # Store current Celery task ID
        task.last_updated = datetime.now(datetime.timezone.utc)
        db.session.commit()
        current_app.logger.info(f"[Task {task_id}] Task status set to 'running'. Celery ID: {self.request.id}")

        source_endpoint = db.session.get(Endpoint, task.source_id)
        target_endpoint = db.session.get(Endpoint, task.destination_id)
        if not source_endpoint or not target_endpoint:
            raise ValueError("Source or Target Endpoint not found.")

        # --- Build Connector Configurations ---
        source_config = build_connector_config(source_endpoint)
        target_config = build_connector_config(target_endpoint)
        # Ensure target schema is passed if available (SQLAlchemy connector might use it)
        target_config['target_schema'] = target_endpoint.target_schema

        # --- Get and Connect Connectors ---
        current_app.logger.info(f"[Task {task_id}] Getting connectors...")
        source_connector = get_source_connector(source_endpoint)
        target_connector = get_target_connector(target_endpoint)

        current_app.logger.info(f"[Task {task_id}] Connecting source ({source_endpoint.type})...")
        source_connector.connect(source_config)
        current_app.logger.info(f"[Task {task_id}] Connecting target ({target_endpoint.type})...")
        target_connector.connect(target_config)

        selected_tables = task.tables or [] # Ensure it's a list
        if not selected_tables:
             raise ValueError("No tables selected for replication.")

        # --- Create Schema and Tables (if configured) ---
        if task.create_tables:
             target_schema_name = target_endpoint.target_schema
             if not target_schema_name:
                 current_app.logger.warning(f"[Task {task_id}] 'Create Tables' is enabled, but target endpoint has no target schema defined. Skipping table creation.")
             else:
                 current_app.logger.info(f"[Task {task_id}] Ensuring target schema '{target_schema_name}' exists...")
                 target_connector.create_schema_if_not_exists(target_schema_name)

                 for table_ref in selected_tables:
                    schema_name, table_name = table_ref['schema'], table_ref['table'] # Assuming structure {'schema': s, 'table': t}
                    current_app.logger.info(f"[Task {task_id}] Getting schema for source table {schema_name}.{table_name}")
                    source_schema_def = source_connector.get_table_schema(schema_name, table_name)
                    current_app.logger.info(f"[Task {task_id}] Creating target table {target_schema_name}.{table_name} if not exists...")
                    # Schema conversion happens within the target connector or needs a converter step here
                    target_connector.create_table_if_not_exists(source_schema_def, target_schema_name) # Pass target schema

        # --- Initial Load (if configured) ---
        if task.initial_load:
            current_app.logger.info(f"[Task {task_id}] Starting initial load...")
            target_schema_name = target_endpoint.target_schema # Get target schema again
            if not target_schema_name:
                 raise ValueError("Initial load requires a target schema to be defined on the target endpoint.")

            for table_ref in selected_tables:
                schema_name, table_name = table_ref['schema'], table_ref['table']
                current_app.logger.info(f"[Task {task_id}] Performing initial load for {schema_name}.{table_name} into {target_schema_name}.{table_name}")

                chunk_iterator = source_connector.perform_initial_load_chunk(schema_name, table_name, chunk_size=1000) # Adjust chunk size

                for chunk in chunk_iterator:
                    # **** CHECK FOR STOP SIGNAL (during initial load) ****
                    if redis_client and stop_key and redis_client.exists(stop_key):
                        current_app.logger.info(f"[Task {task_id}] Stop requested during initial load for {schema_name}.{table_name}.")
                        stop_requested = True
                        break # Exit inner loop (current table)

                    if chunk:
                         target_connector.write_initial_load_chunk(target_schema_name, table_name, chunk)
                         # Update metrics (bytes/rows processed if needed)
                         # db.session.commit() # Commit progress periodically? Be careful with frequency

                if stop_requested:
                    break # Exit outer loop (all tables)

            if stop_requested:
                 current_app.logger.info(f"[Task {task_id}] Initial load stopped by user request.")
                 # The finally block will handle setting status to 'stopped'
            else:
                current_app.logger.info(f"[Task {task_id}] Initial load completed.")
                # Optionally update task state after successful initial load
                task.initial_load = False # Mark initial load as done?
                db.session.commit()

        # Check stop request again before starting CDC loop
        if stop_requested:
             raise Exception("Task stopped after initial load by user request.")

        # --- CDC Loop ---
        current_app.logger.info(f"[Task {task_id}] Starting CDC loop...")
        last_pos = task.last_position # Load last position once
        check_interval_counter = 0 # Counter for periodic checks

        while True:
            check_interval_counter += 1

            # **** PERIODIC CHECK FOR STOP SIGNAL (inside CDC loop) ****
            # Check every N iterations (e.g., 10) or based on time elapsed
            if check_interval_counter % 10 == 0:
                if redis_client and stop_key and redis_client.exists(stop_key):
                    current_app.logger.info(f"[Task {task_id}] Stop requested via Redis signal. Exiting CDC loop.")
                    stop_requested = True
                    break # Exit CDC loop gracefully

                # Optional: Fallback DB status check if Redis fails or is not used
                # try:
                #     db.session.refresh(task)
                #     if task.status == 'stopping':
                #        current_app.logger.info(f"[Task {task_id}] Stop requested via DB status. Exiting CDC loop.")
                #        stop_requested = True
                #        break
                # except SQLAlchemyError as db_refresh_err:
                #      current_app.logger.warning(f"[Task {task_id}] Could not refresh task status from DB: {db_refresh_err}")

            # --- Fetch and Apply Changes ---
            try:
                 changes, new_pos = source_connector.get_changes(last_pos)

                 if changes:
                      current_app.logger.debug(f"[Task {task_id}] Applying {len(changes)} change(s).")
                      target_connector.apply_changes(changes)

                      # --- Update Metrics ---
                      metrics = task.metrics or {} # Ensure metrics dict exists
                      for change in changes:
                          op = change.get('operation')
                          if op == 'insert': metrics['inserts'] = metrics.get('inserts', 0) + 1
                          elif op == 'update': metrics['updates'] = metrics.get('updates', 0) + 1
                          elif op == 'delete': metrics['deletes'] = metrics.get('deletes', 0) + 1
                          # Add bytes processed if available

                      metrics['last_updated'] = datetime.now(datetime.timezone.utc).isoformat()
                      # Calculate latency if timestamps are available in changes
                      # metrics['latency'] = ...
                      task.metrics = metrics

                 if new_pos:
                      last_pos = new_pos
                      task.last_position = last_pos # Update position in task object

                 # Commit position and metrics after applying changes and getting new position
                 db.session.commit()
                 current_app.logger.debug(f"[Task {task_id}] Position and metrics committed. New Position: {last_pos}")

                 # If there were changes, continue loop immediately
                 if changes:
                     check_interval_counter = 0 # Reset check counter after activity
                     continue

                 # If no changes, sleep before next check
                 time.sleep(5) # Adjust sleep interval as needed

            except Exception as loop_exc:
                 current_app.logger.error(f"[Task {task_id}] Error within CDC loop: {loop_exc}", exc_info=True)
                 task.status = 'failed' # Mark as failed immediately on loop error
                 task.last_updated = datetime.now(datetime.timezone.utc)
                 # Optionally add error message to task.metrics or a dedicated field
                 if task.metrics: task.metrics['error'] = str(loop_exc)[:500]
                 db.session.commit()
                 raise # Re-raise to exit the task and trigger finally block

        # --- End of CDC loop (only reachable via 'break') ---
        current_app.logger.info(f"[Task {task_id}] Exited CDC loop.")
        if not stop_requested:
            # This path should ideally not be reached if while True is used correctly
            current_app.logger.warning(f"[Task {task_id}] CDC loop finished unexpectedly (without stop request or error). Setting status to completed.")
            task.status = 'completed'

    except Exception as e:
        # General exception handler for setup phase or errors raised from loops
        current_app.logger.error(f"[Task {task_id}] Replication task execution failed: {e}", exc_info=True)
        if task and task.status not in ['failed', 'stopping']: # Avoid overwriting 'stopping' or if already failed
             task.status = 'failed'
             # Store error message if possible
             if task.metrics: task.metrics['error'] = str(e)[:500]
        # Status will be committed in finally block

    finally:
        current_app.logger.info(f"[Task {task_id}] Entering finally block. Stop requested: {stop_requested}")

        # --- Disconnect Connectors ---
        current_app.logger.info(f"[Task {task_id}] Disconnecting connectors...")
        if source_connector:
            try: source_connector.disconnect()
            except Exception as disc_e: current_app.logger.error(f"[Task {task_id}] Error disconnecting source: {disc_e}", exc_info=True)
        if target_connector:
            try: target_connector.disconnect()
            except Exception as disc_e: current_app.logger.error(f"[Task {task_id}] Error disconnecting target: {disc_e}", exc_info=True)

        # --- Final Status Update and Commit ---
        final_status = 'unknown'
        if task:
            try:
                 # Determine definitive final status
                 if task.status == 'failed': # If failed inside loops, keep it
                     final_status = 'failed'
                 elif stop_requested:
                     final_status = 'stopped'
                 elif task.status == 'running': # If somehow still 'running', mark as completed or failed?
                     # This indicates an issue, maybe mark as failed?
                     final_status = 'completed' # Or 'failed' if unexpected exit
                     current_app.logger.warning(f"[Task {task.id}] Task ended while status was 'running' and not stopped. Setting final status to '{final_status}'.")
                 else:
                     final_status = task.status # Keep 'completed' if set before finally

                 # Update the task object only if the status needs changing
                 if task.status != final_status:
                    task.status = final_status

                 task.last_updated = datetime.now(datetime.timezone.utc)
                 db.session.commit()
                 current_app.logger.info(f"[Task {task.id}] Worker function finished. Final DB status committed: {task.status}")

            except SQLAlchemyError as commit_e:
                 current_app.logger.error(f"[Task {task.id}] Failed to commit final task status '{final_status}': {commit_e}", exc_info=True)
                 db.session.rollback()
            except Exception as final_e:
                  current_app.logger.error(f"[Task {task.id}] Error during final status update: {final_e}", exc_info=True)

        # --- Clean up Redis Stop Flag ---
        # Clean up regardless of success/failure, if key was set
        if redis_client and stop_key:
            try:
                 deleted_count = redis_client.delete(stop_key)
                 current_app.logger.info(f"[Task {task_id}] Cleaned up Redis stop key '{stop_key}'. Deleted: {deleted_count}")
            except Exception as redis_del_e:
                 current_app.logger.warning(f"[Task {task_id}] Failed to clean up Redis stop key '{stop_key}': {redis_del_e}")
