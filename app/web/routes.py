# app/web/routes.py (Final Version & Cleaned + Navigation Enhancements)
from typing import Optional

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app, abort

from app.interfaces import SourceConnector
from app.models import Endpoint, ReplicationTask
from app import db
from app.forms import TaskForm, EndpointForm
from datetime import datetime, timezone # Import timezone
import json
from celery.result import AsyncResult
from sqlalchemy.exc import SQLAlchemyError # Import SQLAlchemyError
from redis import Redis # Import Redis for stop task route

# --- Import the REAL Celery task ---
from app.tasks import run_replication
from app.celery import celery_app
# --- Import helpers needed by API routes ---
from app.replication_worker import build_connector_config, get_source_connector
# Assuming MetadataService is needed for testing connection
from app.services.metadata_service import MetadataService

bp = Blueprint('web', __name__, template_folder='templates')

# --- Dashboard (Tasks View) ---
@bp.route('/')
def dashboard():
    """Displays the main dashboard focused on Replication Tasks."""
    tasks = ReplicationTask.query.order_by(ReplicationTask.created_at.desc()).all()
    breadcrumbs = [{'url': None, 'text': 'Tasks'}]
    return render_template('dashboard.html', tasks=tasks, breadcrumbs=breadcrumbs, current_page='tasks')

# --- Endpoints List View ---
@bp.route('/endpoints')
def list_endpoints():
    """Displays a dedicated page listing all configured Endpoints."""
    endpoints = Endpoint.query.order_by(Endpoint.created_at.desc()).all()
    breadcrumbs = [
        {'url': url_for('web.dashboard'), 'text': 'Tasks'},
        {'url': None, 'text': 'Endpoints'}
    ]
    return render_template('endpoints.html', endpoints=endpoints, breadcrumbs=breadcrumbs, current_page='endpoints')


# --- Endpoint CRUD ---
@bp.route('/endpoint/create', methods=['GET', 'POST'])
def create_endpoint():
    form = EndpointForm()
    if form.validate_on_submit():
        try:
            new_endpoint = Endpoint()
            # Populate common fields first
            new_endpoint.name = form.name.data
            new_endpoint.type = form.type.data
            new_endpoint.endpoint_type = form.endpoint_type.data
            new_endpoint.username = form.username.data
            new_endpoint.password = form.password.data # Consider encrypting password here
            new_endpoint.target_schema = form.target_schema.data if form.endpoint_type.data == 'target' else None

            # Populate type-specific fields
            db_type = form.type.data
            if db_type == 'postgres':
                new_endpoint.host = form.postgres_host.data
                new_endpoint.port = form.postgres_port.data
                new_endpoint.database = form.postgres_database.data
            elif db_type == 'oracle':
                new_endpoint.host = form.oracle_host.data
                new_endpoint.port = form.oracle_port.data
                new_endpoint.service_name = form.oracle_service_name.data
            elif db_type == 'mysql': # Assuming similar fields, adjust form if needed
                new_endpoint.host = form.data.get('mysql_host') # Get fields directly if not added to WTForm object
                new_endpoint.port = form.data.get('mysql_port')
                new_endpoint.database = form.data.get('mysql_database')
            elif db_type == 'bigquery':
                new_endpoint.dataset = form.dataset.data
                new_endpoint.credentials_json = form.credentials_json.data

            db.session.add(new_endpoint)
            db.session.commit()
            flash(f'Endpoint "{new_endpoint.name}" created successfully!', 'success')
            return redirect(url_for('web.list_endpoints'))
        except SQLAlchemyError as e:
             db.session.rollback()
             flash(f"Database error creating endpoint: {e}", "danger")
             current_app.logger.error(f"DB error creating endpoint: {e}", exc_info=True)
        except Exception as e:
            db.session.rollback()
            flash(f"Error creating endpoint: {e}", 'danger')
            current_app.logger.error(f"Error creating endpoint: {e}", exc_info=True)

    breadcrumbs = [
        {'url': url_for('web.dashboard'), 'text': 'Tasks'},
        {'url': url_for('web.list_endpoints'), 'text': 'Endpoints'},
        {'url': None, 'text': 'Create Endpoint'}
    ]
    return render_template('create_endpoint.html', form=form, breadcrumbs=breadcrumbs, current_page='endpoints')


@bp.route('/endpoint/<int:endpoint_id>/edit', methods=['GET', 'POST'])
def edit_endpoint(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint:
        abort(404)
    form = EndpointForm(obj=endpoint) # Pre-populate form

    # Pre-populate type-specific fields (WTForms doesn't handle this automatically with obj=)
    if request.method == 'GET':
        if endpoint.type == 'postgres':
            form.postgres_host.data = endpoint.host
            form.postgres_port.data = endpoint.port
            form.postgres_database.data = endpoint.database
        elif endpoint.type == 'oracle':
            form.oracle_host.data = endpoint.host
            form.oracle_port.data = endpoint.port
            form.oracle_service_name.data = endpoint.service_name
        elif endpoint.type == 'bigquery':
            form.dataset.data = endpoint.dataset
            form.credentials_json.data = endpoint.credentials_json
        # Add elif for mysql if needed

    if form.validate_on_submit():
        try:
            # Populate common fields
            endpoint.name = form.name.data
            # Don't update endpoint.type on edit
            endpoint.endpoint_type = form.endpoint_type.data
            endpoint.username = form.username.data
            if form.password.data: # Only update password if provided
                 endpoint.password = form.password.data # Consider encrypting
            endpoint.target_schema = form.target_schema.data if form.endpoint_type.data == 'target' else None

            # Populate type-specific fields
            if endpoint.type == 'postgres':
                endpoint.host = form.postgres_host.data
                endpoint.port = form.postgres_port.data
                endpoint.database = form.postgres_database.data
            elif endpoint.type == 'oracle':
                endpoint.host = form.oracle_host.data
                endpoint.port = form.oracle_port.data
                endpoint.service_name = form.oracle_service_name.data
            elif endpoint.type == 'bigquery':
                endpoint.dataset = form.dataset.data
                endpoint.credentials_json = form.credentials_json.data
             # Add elif for mysql if needed

            db.session.commit()
            flash(f'Endpoint "{endpoint.name}" updated successfully!', 'success')
            return redirect(url_for('web.list_endpoints'))
        except SQLAlchemyError as e:
             db.session.rollback()
             flash(f"Database error updating endpoint: {e}", "danger")
             current_app.logger.error(f"DB error updating endpoint {endpoint_id}: {e}", exc_info=True)
        except Exception as e:
            db.session.rollback()
            flash(f"Error updating endpoint: {e}", 'danger')
            current_app.logger.error(f"Error updating endpoint {endpoint_id}: {e}", exc_info=True)

    breadcrumbs = [
        {'url': url_for('web.dashboard'), 'text': 'Tasks'},
        {'url': url_for('web.list_endpoints'), 'text': 'Endpoints'},
        {'url': None, 'text': f'Edit Endpoint ({endpoint.name})'}
    ]
    return render_template('edit_endpoint.html', form=form, endpoint=endpoint, breadcrumbs=breadcrumbs, current_page='endpoints')

@bp.route('/endpoint/<int:endpoint_id>/delete', methods=['POST']) # Changed to POST for safety
def delete_endpoint(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint:
         flash('Endpoint not found.', 'warning')
         return redirect(url_for('web.list_endpoints'))
     # Check if endpoint is used by any tasks
    tasks_using_endpoint = ReplicationTask.query.filter(
         (ReplicationTask.source_id == endpoint_id) | (ReplicationTask.destination_id == endpoint_id)
     ).count()

    if tasks_using_endpoint > 0:
         flash(f'Cannot delete endpoint "{endpoint.name}" as it is used by {tasks_using_endpoint} task(s).', 'danger')
         return redirect(url_for('web.list_endpoints'))
    try:
        db.session.delete(endpoint)
        db.session.commit()
        flash(f'Endpoint "{endpoint.name}" deleted successfully.', 'success')
    except SQLAlchemyError as e:
        db.session.rollback()
        flash(f'Database error deleting endpoint: {e}', 'danger')
        current_app.logger.error(f"DB error deleting endpoint {endpoint_id}: {e}", exc_info=True)
    except Exception as e:
        db.session.rollback()
        flash(f'Error deleting endpoint: {e}', 'danger')
        current_app.logger.error(f"Error deleting endpoint {endpoint_id}: {e}", exc_info=True)

    return redirect(url_for('web.list_endpoints'))

# --- Task CRUD ---
@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()
    # Populate endpoint choices dynamically
    endpoints = Endpoint.query.order_by(Endpoint.name).all()
    form.source.choices = [(e.id, f"{e.name} ({e.type})") for e in endpoints if e.endpoint_type == 'source']
    form.destination.choices = [(e.id, f"{e.name} ({e.type})") for e in endpoints if e.endpoint_type == 'target']

    if form.validate_on_submit():
        try:
            selected_tables_json = request.form.get('selected-tables', '[]')
            selected_tables = json.loads(selected_tables_json)

            if not selected_tables:
                 flash("Please select at least one table for replication.", "warning")
                 # Re-render form with choices populated
                 breadcrumbs = [
                    {'url': url_for('web.dashboard'), 'text': 'Tasks'},
                    {'url': None, 'text': 'Create Task'}
                 ]
                 return render_template('create_task.html', form=form, breadcrumbs=breadcrumbs, current_page='tasks')

            new_task = ReplicationTask(
                name=form.name.data,
                source_id=form.source.data,
                destination_id=form.destination.data,
                tables=selected_tables, # Store parsed JSON
                initial_load=form.initial_load.data,
                create_tables=form.create_tables.data,
                status='stopped', # Initial status
                metrics={ # Initialize metrics
                    'inserts': 0, 'updates': 0, 'deletes': 0, 'bytes_processed': 0,
                    'latency': 0, 'last_updated': datetime.now(timezone.utc).isoformat(),
                    'error': None
                 }
            )
            db.session.add(new_task)
            db.session.commit()
            flash(f'Task "{new_task.name}" created successfully!', 'success')
            return redirect(url_for('web.dashboard'))
        except json.JSONDecodeError:
             flash("Error decoding selected tables data.", "danger")
        except SQLAlchemyError as e:
             db.session.rollback()
             flash(f"Database error creating task: {e}", "danger")
             current_app.logger.error(f"DB error creating task: {e}", exc_info=True)
        except Exception as e:
             db.session.rollback()
             flash(f"Error creating task: {e}", 'danger')
             current_app.logger.error(f"Error creating task: {e}", exc_info=True)

    breadcrumbs = [
        {'url': url_for('web.dashboard'), 'text': 'Tasks'},
        {'url': None, 'text': 'Create Task'}
    ]
    return render_template('create_task.html', form=form, breadcrumbs=breadcrumbs, current_page='tasks')

@bp.route('/task/<int:task_id>/edit', methods=['GET', 'POST'])
def edit_task(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        abort(404)
    form = TaskForm(obj=task) # Pre-populate form

    # Populate endpoint choices
    endpoints = Endpoint.query.order_by(Endpoint.name).all()
    form.source.choices = [(e.id, f"{e.name} ({e.type})") for e in endpoints if e.endpoint_type == 'source']
    form.destination.choices = [(e.id, f"{e.name} ({e.type})") for e in endpoints if e.endpoint_type == 'target']
    # Ensure current selection is preserved if obj doesn't map perfectly
    if request.method == 'GET':
         form.source.data = task.source_id
         form.destination.data = task.destination_id

    if form.validate_on_submit():
        try:
            selected_tables_json = request.form.get('selected-tables', '[]')
            selected_tables = json.loads(selected_tables_json)

            if not selected_tables:
                 flash("Please select at least one table for replication.", "warning")
                 breadcrumbs = [ # Needed for re-render
                     {'url': url_for('web.dashboard'), 'text': 'Tasks'},
                     {'url': None, 'text': f'Edit Task ({task.name})'}
                 ]
                 return render_template('edit_task.html', form=form, task=task, tables_json_val=json.dumps(task.tables or []), breadcrumbs=breadcrumbs, current_page='tasks')

            task.name = form.name.data
            task.source_id = form.source.data
            task.destination_id = form.destination.data
            task.tables = selected_tables
            task.initial_load = form.initial_load.data
            task.create_tables = form.create_tables.data
            task.last_updated = datetime.now(timezone.utc) # Update timestamp

            db.session.commit()
            flash(f'Task "{task.name}" updated successfully!', 'success')
            return redirect(url_for('web.dashboard'))

        except json.JSONDecodeError:
             flash("Error decoding selected tables data.", "danger")
        except SQLAlchemyError as e:
             db.session.rollback()
             flash(f"Database error updating task: {e}", "danger")
             current_app.logger.error(f"DB error updating task {task_id}: {e}", exc_info=True)
        except Exception as e:
            db.session.rollback()
            flash(f"Error updating task: {e}", 'danger')
            current_app.logger.error(f"Error updating task {task_id}: {e}", exc_info=True)

    breadcrumbs = [
        {'url': url_for('web.dashboard'), 'text': 'Tasks'},
        {'url': None, 'text': f'Edit Task ({task.name})'}
    ]
    # Pass tables as JSON string for hidden input pre-population
    tables_json_val = json.dumps(task.tables or [])
    return render_template('edit_task.html', form=form, task=task, tables_json_val=tables_json_val, breadcrumbs=breadcrumbs, current_page='tasks')


@bp.route('/task/<int:task_id>/delete', methods=['POST'])
def delete_task(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if task:
        # Add logic here to stop the task if it's running before deleting
        if task.status == 'running' or task.status == 'pending':
             flash(f'Please stop task "{task.name}" before deleting.', 'warning')
             return redirect(url_for('web.dashboard'))
        try:
             db.session.delete(task)
             db.session.commit()
             flash(f'Task "{task.name}" deleted successfully.', 'success')
             return jsonify({"success": True}) # Return JSON for JS handler
        except SQLAlchemyError as e:
             db.session.rollback()
             flash(f'Database error deleting task: {e}', 'danger')
             current_app.logger.error(f"DB error deleting task {task_id}: {e}", exc_info=True)
             return jsonify({"success": False, "message": "Database error."}), 500
        except Exception as e:
             db.session.rollback()
             flash(f'Error deleting task: {e}', 'danger')
             current_app.logger.error(f"Error deleting task {task_id}: {e}", exc_info=True)
             return jsonify({"success": False, "message": "Server error."}), 500
    else:
         flash('Task not found.', 'warning')
         return jsonify({"success": False, "message": "Task not found."}), 404

# --- Task Control API ---
@bp.route('/task/<int:task_id>/run', methods=['POST'])
def run_task_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404

    # Allow running from 'stopped', 'failed', 'completed' states
    if task.status in ['running', 'pending', 'stopping']:
        return jsonify({"success": False, "message": f"Task cannot be started from '{task.status}' state."}), 400

#    if task.status == 'running': return jsonify({"success": False, "message": "Task is already running."}), 400

    # Get optional start datetime from request
 #   data = request.get_json()
 #   start_datetime_str = data.get('start_datetime') if data else None

    try:
        # Update status immediately
        task.status = 'pending' # Or 'queued'
        task.celery_task_id = None # Clear previous ID
        task.last_updated = datetime.now(timezone.utc)
        db.session.commit()

        # Call Celery task asynchronously
        celery_task = run_replication.delay(task_id)
        task.celery_task_id = celery_task.id # Store the new Celery task ID
        db.session.commit()

        flash(f"Task '{task.name}' submitted for execution.", "info")
        return jsonify({"success": True, "message": "Task submitted.", "celery_id": celery_task.id})

    except SQLAlchemyError as e:
         db.session.rollback()
         current_app.logger.error(f"DB error running task {task_id}: {e}", exc_info=True)
         return jsonify({"success": False, "message": "Database error."}), 500
    except Exception as e:
         db.session.rollback()
         # Revert status if failed to submit
         task.status = 'failed' # Or 'stopped'
         db.session.commit()
         current_app.logger.error(f"Error running task {task_id}: {e}", exc_info=True)
         return jsonify({"success": False, "message": f"Failed to submit task: {e}"}), 500

@bp.route('/task/<int:task_id>/reload', methods=['POST'])
def reload_task_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    if task.status in ['running', 'pending', 'stopping']:
        return jsonify({"success": False, "message": "Task is already active. Stop it first to reload."}), 400

    source_connector: Optional[SourceConnector] = None # Initialize connector variable
    try:
        # --- Get Current SCN from Source BEFORE Initial Load ---
        current_app.logger.info(f"[Reload Task {task_id}] Fetching current SCN from source endpoint {task.source_id}...")
        source_endpoint = db.session.get(Endpoint, task.source_id)
        if not source_endpoint:
             raise ValueError("Source endpoint for task not found.")

        source_config = build_connector_config(source_endpoint)
        source_connector = get_source_connector(source_endpoint) # Get connector instance
        source_connector.connect(source_config) # Connect
        current_position = source_connector.get_current_position() # Fetch SCN ({'scn': ...})
        source_connector.disconnect() # Disconnect immediately
        source_connector = None # Clear reference

        if not current_position or not isinstance(current_position.get('scn'), int): # Validate
             raise ValueError("Could not retrieve valid current SCN from source endpoint.")

        pre_load_scn = current_position['scn']
        current_app.logger.info(f"[Reload Task {task_id}] Captured pre-load SCN: {pre_load_scn}. Preparing task for reload.")
        # --- End Get Current SCN ---

        # --- Update Task for Reload ---
        task.status = 'pending'
        task.last_position = current_position # Store the PRE-LOAD position dictionary
        task.initial_load = True # Force initial load in the task
        task.metrics = { # Reset metrics
             'inserts': 0, 'updates': 0, 'deletes': 0, 'bytes_processed': 0,
             'latency': 0, 'last_updated': datetime.now(timezone.utc).isoformat(),
             'error': None
        }
        task.celery_task_id = None # Clear previous ID
        task.last_updated = datetime.now(timezone.utc)
        db.session.commit() # Commit changes before calling task
        # --- End Update Task ---

        # --- Call Celery task ---
        celery_task = run_replication.delay(task_id) # Task will perform init load, then use saved position
        task.celery_task_id = celery_task.id
        db.session.commit() # Save Celery ID

        flash(f"Task '{task.name}' submitted for reload (CDC will start from SCN {pre_load_scn}).", "info")
        return jsonify({"success": True, "message": "Task submitted for reload.", "celery_id": celery_task.id})

    # --- Exception Handling ---
    except SQLAlchemyError as e:
         db.session.rollback()
         current_app.logger.error(f"DB error preparing reload for task {task_id}: {e}", exc_info=True)
         return jsonify({"success": False, "message": "Database error during reload setup."}), 500
    except ImportError as e:
         db.session.rollback()
         current_app.logger.error(f"Import error during reload task {task_id}: {e}", exc_info=True)
         return jsonify({"success": False, "message": f"Connector error: {e}"}), 500
    except Exception as e:
         db.session.rollback()
         # Attempt to revert status if it was set to pending
         if task and task.status == 'pending': task.status = 'failed'; db.session.commit()
         current_app.logger.error(f"Error preparing reload for task {task_id}: {e}", exc_info=True)
         return jsonify({"success": False, "message": f"Failed to prepare reload task: {e}"}), 500
    finally:
         # Ensure connector is disconnected if an error occurred after connection
         if source_connector and source_connector.conn:
              try: source_connector.disconnect()
              except Exception as disconnect_err:
                   current_app.logger.error(f"[Reload Task {task_id}] Error disconnecting source connector during exception handling: {disconnect_err}", exc_info=True)




@bp.route('/task/<int:task_id>/stop', methods=['POST'])
def stop_task_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    # Check if task is in a stoppable state
    if task.status not in ['running', 'pending']:
         return jsonify({"success": False, "message": f"Task is not running or pending (status: {task.status})."}), 400

    try:
        # 1. Update DB status to 'stopping' immediately
        task.status = 'stopping'
        task.last_updated = datetime.now(timezone.utc)
        db.session.commit()
        current_app.logger.info(f"Set task {task_id} status to 'stopping' in database.")

        # 2. Signal the Celery task via Redis flag
        celery_task_id = task.celery_task_id
        if celery_task_id:
            try:
                redis_url = current_app.config.get('CELERY_RESULT_BACKEND')
                if redis_url and redis_url.startswith('redis://'):
                     redis_client = Redis.from_url(redis_url) # Connect to Redis
                     stop_key = f"stop_request:{celery_task_id}"
                     redis_client.set(stop_key, "1", ex=3600) # Set flag with 1-hour expiry
                     current_app.logger.info(f"Sent stop signal via Redis key '{stop_key}' for Celery task {celery_task_id}.")
                else:
                     current_app.logger.warning(f"Redis not configured. Cannot send stop signal for task {task_id}.")
                     # Optionally attempt revoke as fallback if Redis not available
                     # result = AsyncResult(celery_task_id, app=celery_app)
                     # result.revoke(terminate=True, signal='SIGTERM')

            except Exception as e:
                 current_app.logger.error(f"Failed to send stop signal via Redis for Celery task {celery_task_id}: {e}", exc_info=True)
                 # Don't fail the whole request, DB status is already 'stopping'
                 flash("Failed to send stop signal to worker, status set to 'stopping'.", "warning")

        else:
             # If no celery id, maybe it finished between check and stop? Set to stopped.
             current_app.logger.warning(f"No Celery task ID found for task {task_id} while trying to stop. Setting status to 'stopped'.")
             task.status = 'stopped'
             db.session.commit()


        flash(f"Stop request sent for task '{task.name}'.", "info")
        return jsonify({"success": True, "message": "Stop request sent."})

    except SQLAlchemyError as e:
        db.session.rollback()
        current_app.logger.error(f"Database error processing stop request for task {task_id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Database error processing stop request."}), 500
    except Exception as e:
        current_app.logger.error(f"Unexpected error processing stop request for task {task_id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Unexpected error processing stop request."}), 500

# app/web/routes.py

@bp.route('/task/<int:task_id>/status', methods=['GET'])
def get_task_status_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404

    # Only return data directly from the ReplicationTask model
    status_data = {
        "success": True,
        "task_id": task.id,
        "status": task.status, # Status from DB
        "last_updated": task.last_updated.isoformat() if task.last_updated else None,
        "last_position": task.last_position,
        "metrics": task.metrics or {}
        # REMOVED Celery status check section
    }
    return jsonify(status_data)

#@bp.route('/task/<int:task_id>/status', methods=['GET'])
#def get_task_status_api(task_id):
#    task = db.session.get(ReplicationTask, task_id)
#    if not task: return jsonify({"success": False, "message": "Task not found."}), 404

#    status_data = {
#        "success": True,
#        "task_id": task.id,
#        "status": task.status, # Status from DB
#        "last_updated": task.last_updated.isoformat() if task.last_updated else None,
#        "last_position": task.last_position,
#        "metrics": task.metrics or {}
#    }
#    # --- Query Celery result backend for supplementary info ---
#    celery_task_id = task.celery_task_id
#    if celery_task_id:
#        try:
#            # Get Celery app instance from current_app if needed
#            # If celery_app is imported directly, use it:
#            async_result = AsyncResult(celery_task_id, app=celery_app) # Pass app instance or use imported celery_app
#            status_data["celery_status"] = async_result.status # PENDING, STARTED, SUCCESS, FAILURE etc.
#            if async_result.failed():
#                 status_data["celery_error"] = str(async_result.info) # Or async_result.traceback
#            elif async_result.status == 'PROGRESS': # If task reports progress
#                status_data["celery_progress"] = async_result.info
#        except Exception as e:
#            current_app.logger.warning(f"Could not fetch Celery status for task {task.id} (Celery ID: {celery_task_id}): {e}")
#            status_data["celery_status"] = "UNKNOWN" # Indicate Celery status couldn't be fetched
#
#    return jsonify(status_data)


# --- Metadata API ---
@bp.route('/api/endpoints/<int:endpoint_id>/tables', methods=['GET'])
def get_endpoint_tables(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint:
        return jsonify({"error": "Endpoint not found"}), 404

    try:
        schemas_with_tables = {} # Initialize empty dict

        # --- Call the correct MetadataService method based on type ---
        if endpoint.type == 'postgres':
            # Assuming _get_postgres_schemas returns {schema: [tables]}
            schemas_with_tables = MetadataService._get_postgres_schemas(endpoint)
        elif endpoint.type == 'oracle':
            # Assuming you have a similar method for Oracle
            # You might need to create _get_oracle_schemas_and_tables if it doesn't exist
            schemas_with_tables = MetadataService._get_oracle_schemas_and_tables(endpoint) # Change if method name is different
        elif endpoint.type == 'mysql':
            # Add logic for MySQL
             schemas_with_tables = MetadataService._get_mysql_schemas_and_tables(endpoint) # Create this method if needed
        # Add other endpoint types (e.g., bigquery) as needed
        else:
            raise NotImplementedError(f"Metadata fetching not implemented for endpoint type: {endpoint.type}")
        # --- End Type-Specific Call ---

        # Flatten the result for the JavaScript: list of {'schema': s, 'table': t}
        tables_list = []
        if schemas_with_tables: # Check if data was actually returned
             for schema, tables in schemas_with_tables.items():
                  if tables: # Check if tables list is not empty
                     for table in tables:
                         tables_list.append({'schema': schema, 'table': table})

        # Sort the list for consistent UI display (optional)
        tables_list.sort(key=lambda x: (x['schema'], x['table']))

        current_app.logger.info(f"Successfully fetched {len(tables_list)} tables for endpoint {endpoint_id}")
        return jsonify(tables_list)

    except NotImplementedError as nie:
        current_app.logger.warning(f"Metadata fetching not implemented for endpoint {endpoint_id} (type: {endpoint.type}): {nie}")
        return jsonify({"error": str(nie)}), 501 # 501 Not Implemented
    except Exception as e:
        # Log the specific error from MetadataService or connection
        current_app.logger.error(f"Failed to fetch tables for endpoint {endpoint_id} (type: {endpoint.type}): {e}", exc_info=True)
        # Return a more specific error message if possible, otherwise generic
        return jsonify({"error": f"Failed to fetch tables: An internal error occurred. Check server logs."}), 500

@bp.route('/api/tables/<int:endpoint_id>/<schema_name>/<table_name>/columns', methods=['GET'])
def get_table_columns(endpoint_id, schema_name, table_name):
     endpoint = db.session.get(Endpoint, endpoint_id)
     if not endpoint:
         return jsonify({"error": "Endpoint not found"}), 404
     try:
         # Use MetadataService
         columns = MetadataService.get_table_columns(endpoint, schema_name, table_name)
         return jsonify(columns) # Assuming service returns list of {'name': ..., 'type': ...}
     except Exception as e:
         current_app.logger.error(f"Failed to fetch columns for {schema_name}.{table_name} on endpoint {endpoint_id}: {e}", exc_info=True)
         return jsonify({"error": f"Failed to fetch columns: {e}"}), 500

# --- Connection Test API ---
@bp.route('/api/test_connection', methods=['POST'])
def test_connection():
    if not request.is_json:
        return jsonify({"success": False, "message": "Invalid request: Content-Type must be application/json"}), 415

    form_data = request.get_json()
    if not form_data:
        return jsonify({"success": False, "message": "Invalid request: No JSON data received."}), 400

    current_app.logger.info(f"Testing connection for endpoint: '{form_data.get('name', 'TEMP')}', type: {form_data.get('type')}")

    try:
        endpoint_type = form_data.get('type')
        temp_data = {
            'id': None,
            'name': form_data.get('name', 'TEMP'),
            'type': endpoint_type,
            'username': form_data.get('username'),
            'password': form_data.get('password'),
            'host': None,
            'port': None,
            'database': None,
            'service_name': None,
            'dataset': None,
            'credentials_json': None
        }

        # Populate type-specific fields
        if endpoint_type == 'postgres':
            temp_data.update({
                'host': form_data.get('postgres_host'),
                'port': form_data.get('postgres_port'),
                'database': form_data.get('postgres_database')
            })
        elif endpoint_type == 'oracle':
            temp_data.update({
                'host': form_data.get('oracle_host'),
                'port': form_data.get('oracle_port'),
                'service_name': form_data.get('oracle_service_name')
            })
        # Add other types as needed

        endpoint_temp = Endpoint(**temp_data)
        success, message = MetadataService.test_connection(endpoint_temp)
        return jsonify({"success": success, "message": message})

    except Exception as e:
        current_app.logger.error(f"Connection test failed: {str(e)}", exc_info=True)
        return jsonify({"success": False, "message": f"Connection test error: {str(e)}"})