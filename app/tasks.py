# app/tasks.py (Enhanced with Explicit App Creation)

import time
from datetime import datetime, timezone
import logging
from typing import Dict, Any, Optional

from celery import Celery
# Remove direct import of current_app if no longer needed at module level
# from flask import Flask, current_app
from sqlalchemy.exc import SQLAlchemyError
from redis import Redis

# Assuming interfaces.py is in app/
from app.interfaces import SourceConnector, TargetConnector
# Import necessary components from your app package
from app import db, create_app # *** Import create_app factory ***
from app.models import ReplicationTask, Endpoint
from app.replication_worker import build_connector_config, get_source_connector, get_target_connector

# --- Celery App Definition and Init (Keep as before) ---
celery_app = Celery(__name__)
logger = logging.getLogger(__name__) # Use standard logger

# Keep init_celery if it's defined here, otherwise it's likely in celery.py
# def init_celery(app: Flask): ... (definition with ContextTask)

# --- Task Definition ---
@celery_app.task(bind=True)
def run_replication(self, task_id: int):
    """
    Celery task for replication. Uses standard logging and EXPLICIT app context creation.
    """
    # --- Explicitly Create Flask App Instance for this Task ---
    # This ensures config and extensions are loaded for this task run.
    # Note: Has overhead compared to ContextTask if ContextTask works reliably.
    flask_app = create_app()
    # --- End Explicit App Creation ---

    # *** Wrap the entire task logic (or sections needing context)
    #     in a context block using the created app instance ***
    with flask_app.app_context():
        logger.info(f"[Task {task_id}] Entered application context for task execution. Celery ID: {self.request.id}")

        # --- Initialize variables ---
        task: Optional[ReplicationTask] = None
        source_endpoint: Optional[Endpoint] = None
        target_endpoint: Optional[Endpoint] = None
        source_connector: Optional[SourceConnector] = None
        target_connector: Optional[TargetConnector] = None
        stop_requested = False
        redis_client = None
        stop_key = None

        try:
            # --- Connect to Redis ---
            # Access config via the created flask_app instance
            try:
                redis_url = flask_app.config.get('CELERY_RESULT_BACKEND')
                if redis_url and redis_url.startswith('redis://'):
                     redis_client = Redis.from_url(redis_url, decode_responses=True)
                     stop_key = f"stop_request:{self.request.id}"
                     redis_client.delete(stop_key)
                     logger.info(f"[Task {task_id}] Connected to Redis. Stop key set to: '{stop_key}'")
                else:
                     logger.warning(f"[Task {task_id}] Redis backend not configured/invalid URL. Stop signal check via Redis disabled.")
            except Exception as redis_e:
                 logger.warning(f"[Task {task_id}] Could not connect to Redis for stop signal check: {redis_e}", exc_info=True)
                 redis_client = None

            # --- Fetch Task and Endpoints (already inside context) ---
            logger.info(f"[Task {task_id}] Fetching task and endpoint details from DB.")
            task = db.session.get(ReplicationTask, task_id) # db session uses the context

            if not task:
                logger.error(f"[Task {task_id}] ReplicationTask not found in database. Aborting.")
                return

            if task.status in ['stopped', 'failed', 'completed', 'stopping']:
                 logger.warning(f"[Task {task_id}] Task found in status '{task.status}'. Aborting run.")
                 return

            logger.info(f"[Task {task_id}] Setting task status to 'running'.")
            task.status = 'running'
            task.celery_task_id = self.request.id
            task.last_updated = datetime.now(timezone.utc)
            if task.metrics and 'error' in task.metrics: task.metrics['error'] = None
            db.session.commit()
            logger.info(f"[Task {task_id}] Task status set to 'running'. Celery ID: {self.request.id}")

            source_endpoint = db.session.get(Endpoint, task.source_id)
            target_endpoint = db.session.get(Endpoint, task.destination_id)
            # --- Exit initial DB context implicitly at end of 'with' block ---
            # (but we are wrapping the whole function, so context persists)

            if not source_endpoint or not target_endpoint:
                 raise ValueError(f"[Task {task_id}] Source or Target Endpoint not found in DB.")

            # --- Build Connector Configs / Connect ---
            # These likely don't need the app context themselves, unless connectors use current_app
            logger.info(f"[Task {task_id}] Building connector configurations.")
            source_config = build_connector_config(source_endpoint)
            target_config = build_connector_config(target_endpoint)
            target_config['target_schema'] = target_endpoint.target_schema

            logger.info(f"[Task {task_id}] Getting connectors...")
            source_connector = get_source_connector(source_endpoint)
            target_connector = get_target_connector(target_endpoint)
            logger.info(f"[Task {task_id}] Connecting source ({source_endpoint.type})...")
            source_connector.connect(source_config)
            logger.info(f"[Task {task_id}] Source connected.")
            logger.info(f"[Task {task_id}] Connecting target ({target_endpoint.type})...")
            target_connector.connect(target_config)
            logger.info(f"[Task {task_id}] Target connected.")

            selected_tables = task.tables or []
            if not selected_tables:
                 raise ValueError(f"[Task {task_id}] No tables selected for replication.")

            # --- Create Schema / Tables ---
            # If connector methods use db.session or current_app, they benefit from the context
            if task.create_tables:
                 logger.info(f"[Task {task_id}] Create tables option is enabled.")
                 target_schema_name = target_endpoint.target_schema
                 if not target_schema_name:
                     logger.warning(f"[Task {task_id}] 'Create Tables' enabled, but no target schema set. Skipping.")
                 else:
                     logger.info(f"[Task {task_id}] Ensuring target schema '{target_schema_name}' exists...")
                     target_connector.create_schema_if_not_exists(target_schema_name)
                     for table_ref in selected_tables:
                         schema_name, table_name = table_ref['schema'], table_ref['table']
                         logger.info(f"[Task {task_id}] Getting schema for source table {schema_name}.{table_name}")
                         source_schema_def = source_connector.get_table_schema(schema_name, table_name)
                         logger.info(f"[Task {task_id}] Creating target table {target_schema_name}.{table_name} if not exists...")
                         target_connector.create_table_if_not_exists(source_schema_def, target_schema_name)

            # --- Initial Load ---
            if task.initial_load:
                logger.info(f"[Task {task_id}] Starting initial load process...")
                target_schema_name = target_endpoint.target_schema
                if not target_schema_name:
                     raise ValueError("[Task {task_id}] Initial load requires target schema.")

                for table_ref in selected_tables:
                    # ... (load chunks for table_ref) ...
                    schema_name, table_name = table_ref['schema'], table_ref['table']
                    logger.info(
                        f"[Task {task_id}] Processing initial load for source {schema_name}.{table_name} -> target {target_schema_name}.{table_name}")

                    # *** TRUNCATE/CLEAR TARGET TABLE ***
                    cleared_successfully = False  # Flag to track success
                    try:
                        logger.info(
                            f"[Task {task_id}] Clearing target table {target_schema_name}.{table_name} before initial load...")
                        target_connector.truncate_table(target_schema_name, table_name)
                        # Assuming truncate_table now returns normally if skipped, or raises error on failure
                        cleared_successfully = True  # Assume success if no exception
                        logger.info(
                            f"[Task {task_id}] Target table {target_schema_name}.{table_name} clear operation completed (may have been skipped if table didn't exist).")
                    except Exception as clear_err:
                        # If clearing fails (e.g., permissions error on DELETE), log error and stop task
                        logger.error(
                            f"[Task {task_id}] Failed during target table clear operation for {target_schema_name}.{table_name}: {clear_err}",
                            exc_info=True)
                        raise  # Stop the task by re-raising

                    # *** Proceed only if clear appeared successful (didn't raise error) ***
                    if cleared_successfully:
                        # --- Proceed with fetching and writing chunks ---
                        logger.info(f"[Task {task_id}] Performing chunked load for {schema_name}.{table_name}...")
                        chunk_iterator = source_connector.perform_initial_load_chunk(
                            schema_name=schema_name,
                            table_name=table_name,
                            chunk_size=1000
                        )

                    chunk_count = 0
                    for chunk in chunk_iterator:
                         chunk_count += 1
                         # Check stop signal
                         if redis_client and stop_key and redis_client.exists(stop_key):
                              logger.info(f"[Task {task_id}] Stop requested during initial load chunk {chunk_count}.")
                              stop_requested = True
                              break
                         # Write chunk (connector might use context implicitly)
                         if chunk:
                              logger.debug(f"[Task {task_id}] Writing initial load chunk {chunk_count}...")
                              target_connector.write_initial_load_chunk(target_schema_name, table_name, chunk)
                    if stop_requested: break # Break outer table loop

                if stop_requested:
                     logger.info(f"[Task {task_id}] Initial load stopped prematurely by request.")
                     raise Exception("Task stopped during initial load by user request.")
                else:
                    logger.info(f"[Task {task_id}] Initial load process completed.")
                    # Commit status update (needs context - already have it)
                    task.initial_load = False
                    db.session.commit()
                    logger.info(f"[Task {task_id}] Initial load completion status committed.")

            # --- CDC Loop ---
            if not stop_requested:
                logger.info(f"[Task {task_id}] Starting CDC loop...")
                last_pos = task.last_position
                check_interval_counter = 0
                while True:
                    check_interval_counter += 1
                    logger.debug(f"[Task {task_id}] CDC Loop iteration {check_interval_counter}...")
                    # Check stop signal
                    if redis_client and stop_key and redis_client.exists(stop_key):
                         logger.info(f"[Task {task_id}] Stop requested via Redis signal. Breaking CDC loop.")
                         stop_requested = True
                         break

                    try:
                        # Get changes
                        logger.debug(f"[Task {task_id}] Calling source_connector.get_changes...")
                        changes, new_pos = source_connector.get_changes(last_pos)
                        logger.debug(f"[Task {task_id}] get_changes returned {len(changes)} changes.")

                        if changes or (new_pos and new_pos != last_pos):
                             # Apply changes (connector might use context)
                             if changes:
                                 logger.info(f"[Task {task_id}] Applying {len(changes)} change(s).")
                                 target_connector.apply_changes(changes)

                             # Update metrics & position in task object
                             metrics = task.metrics or {}
                             for change in changes:
                                 op = change.get('operation')
                                 if op == 'insert': metrics['inserts'] = metrics.get('inserts', 0) + 1
                                 elif op == 'update': metrics['updates'] = metrics.get('updates', 0) + 1
                                 elif op == 'delete': metrics['deletes'] = metrics.get('deletes', 0) + 1
                             metrics['last_updated'] = datetime.now(timezone.utc).isoformat()
                             task.metrics = metrics

                             if new_pos and new_pos != last_pos:
                                 last_pos = new_pos
                                 task.last_position = last_pos

                             # Commit changes (needs context - already have it)
                             logger.debug(f"[Task {task_id}] Committing metrics and position changes...")
                             db.session.commit()
                             logger.debug(f"[Task {task_id}] Commit successful.")

                             if changes: continue # Check for more changes immediately

                        # No changes, sleep
                        logger.debug(f"[Task {task_id}] No changes detected, sleeping...")
                        time.sleep(5)

                    except Exception as loop_exc:
                         logger.error(f"[Task {task_id}] Error within CDC loop processing: {loop_exc}", exc_info=True)
                         # Mark failed (needs context - already have it)
                         task.status = 'failed'
                         task.last_updated = datetime.now(timezone.utc)
                         if task.metrics: task.metrics['error'] = str(loop_exc)[:1000]
                         db.session.commit()
                         raise

        except Exception as e:
            logger.error(f"[Task {task_id}] Replication task execution failed in main try block: {e}", exc_info=True)
            # Attempt to mark as failed (needs context - already have it)
            if task and task.status not in ['failed', 'stopping']:
                 task.status = 'failed'
                 if task.metrics: task.metrics['error'] = str(e)[:1000]
                 task.last_updated = datetime.now(timezone.utc)
                 try: db.session.commit()
                 except Exception as fail_commit_e:
                      logger.error(f"[Task {task_id}] Failed to commit 'failed' status during exception handling: {fail_commit_e}")
                      db.session.rollback()


        finally:

            # Logger should be defined earlier in the function using logging.getLogger

            logger.info(
                f"[Task {task_id}] Entering finally block. Stop requested flag: {stop_requested}. Current DB status before final logic: {task.status if task else 'N/A'}")

            # --- Disconnect Connectors ---

            # No app context needed here unless disconnect methods use current_app/db

            logger.info(f"[Task {task_id}] Attempting to disconnect connectors...")

            if source_connector:

                logger.info(f"[Task {task_id}] Disconnecting source connector...")

                try:

                    source_connector.disconnect()

                    logger.info(f"[Task {task_id}] Source connector disconnected.")

                except Exception as disc_e:

                    logger.error(f"[Task {task_id}] Error disconnecting source: {disc_e}", exc_info=True)

            else:

                logger.info(f"[Task {task_id}] Source connector was not initialized, skipping disconnect.")

            if target_connector:

                logger.info(f"[Task {task_id}] Disconnecting target connector...")

                try:

                    target_connector.disconnect()

                    logger.info(f"[Task {task_id}] Target connector disconnected.")

                except Exception as disc_e:

                    logger.error(f"[Task {task_id}] Error disconnecting target: {disc_e}", exc_info=True)

            else:

                logger.info(f"[Task {task_id}] Target connector was not initialized, skipping disconnect.")

            logger.info(f"[Task {task_id}] Finished disconnecting connectors.")

            # --- Final Status Update and Commit (Needs app context - already have it) ---

            final_status = 'unknown'  # Default safety value

            if task:  # Check if task object was loaded successfully earlier

                logger.info(f"[Task {task_id}] Determining final status before commit in finally block...")

                # Refresh task state from DB to get the most recent status

                # in case an error occurred and set it to 'failed' already.

                try:

                    db.session.expire(task)  # Mark object as expired

                    task = db.session.get(ReplicationTask, task_id)  # Re-fetch latest state

                    if not task:

                        logger.error(f"[Task {task_id}] Task disappeared from DB before final commit!")

                        final_status = 'unknown_disappeared'

                    else:

                        current_db_status = task.status

                        logger.info(
                            f"[Task {task_id}] Refreshed DB status is '{current_db_status}'. Stop requested flag: {stop_requested}.")

                        # Determine the final state based on what happened

                        if current_db_status == 'failed':

                            final_status = 'failed'  # Keep failed status if set during run

                        elif stop_requested:

                            final_status = 'stopped'  # If stop was requested, mark as stopped

                        elif current_db_status == 'stopping':

                            # If API set to stopping, but signal wasn't fully processed, mark as stopped

                            final_status = 'stopped'

                            logger.warning(
                                f"[Task {task_id}] Task was 'stopping' but internal stop_requested flag was False. Setting to 'stopped'.")

                        elif current_db_status == 'running':

                            # If loop exited but wasn't stopped or failed, consider it incomplete/failed

                            final_status = 'failed'

                            logger.warning(
                                f"[Task {task.id}] Task ended while status was 'running' and not stopped/failed. Setting final status to 'failed'.")

                        else:

                            # Keep other terminal states like 'completed' if somehow set

                            final_status = current_db_status


                except Exception as refresh_err:

                    logger.error(
                        f"[Task {task_id}] Error refreshing task from DB in finally block: {refresh_err}. Proceeding with potentially stale status.")

                    # Fallback logic using potentially stale task status before refresh attempt

                    if task.status == 'failed':
                        final_status = 'failed'

                    elif stop_requested:
                        final_status = 'stopped'

                    elif task.status == 'stopping':
                        final_status = 'stopped'

                    elif task.status == 'running':
                        final_status = 'failed'

                    else:
                        final_status = task.status  # Keep existing terminal state

                logger.info(f"[Task {task_id}] Final status determined as '{final_status}'.")

                # Commit only if task exists and status needs changing

                if task and task.status != final_status:

                    logger.info(
                        f"[Task {task.id}] Current DB status '{task.status}' differs from final status '{final_status}'. Attempting commit.")

                    task.status = final_status

                    task.last_updated = datetime.now(timezone.utc)  # Update timestamp

                    # Ensure celery_task_id is cleared if stopped/failed/completed? Optional.

                    # task.celery_task_id = None

                    try:

                        db.session.commit()  # Commit requires app context

                        logger.info(f"[Task {task.id}] Final status commit successful. New status: {task.status}")

                    except Exception as commit_e:

                        logger.error(
                            f"[Task {task.id}] Failed to commit final task status '{final_status}': {commit_e}",
                            exc_info=True)

                        db.session.rollback()  # Rollback on commit error

                elif task:

                    logger.info(
                        f"[Task {task.id}] Final status ('{final_status}') matches DB status ('{task.status}'). No commit needed.")

                # else: task became None during refresh error, logged earlier


            else:  # Task object was None when entering finally block (e.g., failed to load initially)

                logger.warning(f"[Task {task_id}] Task object not available in finally block for final status commit.")

            # --- Clean up Redis Stop Flag (No context needed) ---

            if redis_client and stop_key:

                logger.info(f"[Task {task_id}] Attempting to clean up Redis key '{stop_key}'...")

                try:

                    deleted_count = redis_client.delete(stop_key)

                    logger.info(f"[Task {task_id}] Cleaned up Redis stop key '{stop_key}'. Deleted: {deleted_count}")

                except Exception as redis_del_e:

                    logger.warning(f"[Task {task_id}] Failed to clean up Redis stop key '{stop_key}': {redis_del_e}")

            else:

                logger.info(f"[Task {task_id}] No Redis client or stop key to clean up.")

            logger.info(f"[Task {task_id}] Worker function finished execution.")

        # *** The 'with flask_app.app_context():' block ends after this finally block ***
