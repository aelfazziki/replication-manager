# app/web/routes.py (Final Version & Cleaned)

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app, abort
from app.models import Endpoint, ReplicationTask
from app import db
from app.forms import TaskForm, EndpointForm
from datetime import datetime, timezone # Import timezone for create_task default metrics
import json
from celery.result import AsyncResult

# --- Import the REAL Celery task ---
from app.tasks import run_replication

# --- Import helpers needed by API routes ---
from app.replication_worker import build_connector_config, get_source_connector

bp = Blueprint('web', __name__, template_folder='templates')

# --- Dashboard ---
@bp.route('/')
def dashboard():
    tasks = ReplicationTask.query.order_by(ReplicationTask.created_at.desc()).all()
    endpoints = Endpoint.query.order_by(Endpoint.created_at.desc()).all()
    return render_template('dashboard.html', tasks=tasks, endpoints=endpoints)

# --- Endpoint CRUD ---
@bp.route('/endpoint/create', methods=['GET', 'POST'])
def create_endpoint():
    form = EndpointForm()
    if form.validate_on_submit():
        try:
            # Simplified - relies on form structure matching model somewhat
            new_endpoint = Endpoint()
            form.populate_obj(new_endpoint) # Populate common fields

            # Handle type-specific fields based on form data
            db_type = form.type.data
            if db_type == 'postgres':
                new_endpoint.host = form.postgres_host.data
                new_endpoint.port = form.postgres_port.data
                new_endpoint.database = form.postgres_database.data
                new_endpoint.service_name = None # Clear non-relevant fields
            elif db_type == 'oracle':
                new_endpoint.host = form.oracle_host.data
                new_endpoint.port = form.oracle_port.data
                new_endpoint.service_name = form.oracle_service_name.data
                new_endpoint.database = None # Clear non-relevant fields
            # Add logic for mysql, bigquery if fields exist in form
            elif db_type == 'mysql':
                 new_endpoint.host = form.mysql_host.data
                 new_endpoint.port = form.mysql_port.data
                 new_endpoint.database = form.mysql_database.data
                 new_endpoint.service_name = None
            elif db_type == 'bigquery':
                 new_endpoint.dataset = form.dataset.data
                 new_endpoint.credentials_json = form.credentials_json.data
                 # Clear non-relevant fields
                 new_endpoint.host = None
                 new_endpoint.port = None
                 new_endpoint.service_name = None
                 new_endpoint.database = None

            if form.endpoint_type.data == 'source':
                new_endpoint.target_schema = None # Clear target schema for source

            # TODO: Consider encrypting password before saving
            # new_endpoint.password = encrypt(form.password.data)

            db.session.add(new_endpoint)
            db.session.commit()
            flash('Endpoint created successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating endpoint: {str(e)}', 'danger')
            current_app.logger.error(f"Error creating endpoint: {str(e)}", exc_info=True)
    return render_template('create_endpoint.html', form=form, endpoint=None)

@bp.route('/endpoint/<int:endpoint_id>/edit', methods=['GET', 'POST'])
def edit_endpoint(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint: abort(404)
    form = EndpointForm(obj=endpoint)

    # Manually set the type since the field might be disabled in template
    form.type.data = endpoint.type

    if request.method == 'GET':
        # Pre-populate type-specific fields based on saved endpoint data
        # This ensures the correct fields are shown in the form initially
        if endpoint.type == 'postgres':
            form.postgres_host.data = endpoint.host
            form.postgres_port.data = endpoint.port
            form.postgres_database.data = endpoint.database
        elif endpoint.type == 'oracle':
            form.oracle_host.data = endpoint.host
            form.oracle_port.data = endpoint.port
            form.oracle_service_name.data = endpoint.service_name
        # Add other types (mysql, bigquery) here if needed

    if form.validate_on_submit():
        try:
            # Use populate_obj for common fields, then handle type-specific ones
            # Store original password if field is empty, otherwise update
            original_password = endpoint.password
            form.populate_obj(endpoint) # Overwrites endpoint with form data
            if not form.password.data: # If password field left blank, keep original
                 endpoint.password = original_password
            # else: Consider re-encrypting if password changed

            # Handle type-specific fields based on endpoint.type (which cannot change)
            if endpoint.type == 'postgres':
                endpoint.host = form.postgres_host.data
                endpoint.port = form.postgres_port.data
                endpoint.database = form.postgres_database.data
                endpoint.service_name = None
            elif endpoint.type == 'oracle':
                endpoint.host = form.oracle_host.data
                endpoint.port = form.oracle_port.data
                endpoint.service_name = form.oracle_service_name.data
                endpoint.database = None
            # Add logic for mysql, bigquery etc.
            elif endpoint.type == 'mysql':
                 endpoint.host = form.mysql_host.data
                 endpoint.port = form.mysql_port.data
                 endpoint.database = form.mysql_database.data
                 endpoint.service_name = None
            elif endpoint.type == 'bigquery':
                 endpoint.dataset = form.dataset.data
                 endpoint.credentials_json = form.credentials_json.data
                 endpoint.host = None; endpoint.port = None; endpoint.service_name = None; endpoint.database = None

            if endpoint.endpoint_type == 'source':
                endpoint.target_schema = None

            db.session.commit()
            flash('Endpoint updated successfully', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error updating endpoint: {str(e)}', 'danger')
            current_app.logger.error(f"Endpoint update error: {str(e)}", exc_info=True)

    return render_template('edit_endpoint.html', form=form, endpoint=endpoint)

@bp.route('/endpoint/<int:endpoint_id>/delete', methods=['POST'])
def delete_endpoint(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint:
        return jsonify({"success": False, "message": "Endpoint not found"}), 404
    task_count = ReplicationTask.query.filter(
        (ReplicationTask.source_id == endpoint_id) | (ReplicationTask.destination_id == endpoint_id)
    ).count()
    if task_count > 0:
        return jsonify({"success": False, "message": f"Cannot delete: Used by {task_count} task(s)."}), 400
    try:
        db.session.delete(endpoint)
        db.session.commit()
        return jsonify({"success": True, "message": "Endpoint deleted successfully"}), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error deleting endpoint {endpoint_id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"Error deleting endpoint: {str(e)}"}), 500

# --- Task CRUD ---
@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()
    # Populate choices dynamically, ensuring IDs are strings for SelectField
    source_endpoints = Endpoint.query.filter_by(endpoint_type='source').order_by(Endpoint.name).all()
    target_endpoints = Endpoint.query.filter_by(endpoint_type='target').order_by(Endpoint.name).all()
    form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]

    if form.validate_on_submit():
        try:
            selected_tables_json = request.form.get('selected-tables', '[]')
            try:
                tables_data = json.loads(selected_tables_json)
                if not isinstance(tables_data, list): raise ValueError("Table data must be a list.")
            except (json.JSONDecodeError, ValueError) as e:
                flash(f'Invalid table selection format: {e}', 'danger')
                # Re-populate choices before re-rendering
                form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
                form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]
                return render_template('create_task.html', form=form, task=None)

            default_metrics = {
                'inserts': 0, 'updates': 0, 'deletes': 0, 'bytes_processed': 0,
                'latency': 0, 'last_updated': datetime.now(timezone.utc).isoformat(), # Use timezone aware
                'last_position': None
            }
            # Ensure model fields exist or handle potential AttributeError
            new_task = ReplicationTask(
                name=form.name.data,
                source_id=int(form.source.data),
                destination_id=int(form.destination.data),
                status='stopped',
                tables=tables_data,
                initial_load=form.initial_load.data,
                create_tables=form.create_tables.data,
                cdc_config=getattr(form, 'cdc_config', {}), # Example default
                options=getattr(form, 'options', {}), # Example default
                metrics=default_metrics,
                last_position=None
            )
            db.session.add(new_task)
            db.session.commit()
            flash('Task created successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating task: {str(e)}', 'danger')
            current_app.logger.error(f"Error creating task: {str(e)}", exc_info=True)

    # Ensure choices are set on initial GET or validation fail
    form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]
    return render_template('create_task.html', form=form, task=None)

# routes.py - Corrected edit_task function block

@bp.route('/task/edit/<int:task_id>', methods=['GET', 'POST'])
def edit_task(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task: abort(404)
    # Pass existing task object to pre-populate form fields matched by name
    form = TaskForm(obj=task)

    # Populate choices for dropdowns
    source_endpoints = Endpoint.query.filter_by(endpoint_type='source').order_by(Endpoint.name).all()
    target_endpoints = Endpoint.query.filter_by(endpoint_type='target').order_by(Endpoint.name).all()
    form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]

    if form.validate_on_submit():
        try:
            # --- Explicitly update task attributes ---
            task.name = form.name.data
            # Assign selected IDs (converted to int) to the foreign key fields
            task.source_id = int(form.source.data)
            task.destination_id = int(form.destination.data)
            task.initial_load = form.initial_load.data
            task.create_tables = form.create_tables.data
            # Update other fields if they exist in the form (e.g., options)
            # task.options = json.loads(form.options.data) if hasattr(form, 'options') else task.options

            # Handle tables JSON from hidden input
            selected_tables_json = request.form.get('selected-tables', '[]')
            try:
                tables_data = json.loads(selected_tables_json)
                if not isinstance(tables_data, list): raise ValueError("Table data must be a list.")
                task.tables = tables_data
            except (json.JSONDecodeError, ValueError) as e:
                # Handle JSON error - flash message and re-render form
                flash(f'Invalid table selection format: {e}', 'danger')
                # Repopulate choices before re-rendering
                form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
                form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]
                tables_json_val = json.dumps(task.tables) if task.tables is not None else '[]'
                return render_template('edit_task.html', form=form, task=task, tables_json_val=tables_json_val)
            # --- End explicit update ---

            db.session.commit()
            flash('Task updated successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error updating task: {str(e)}', 'danger')
            current_app.logger.error(f"Task update error: {str(e)}", exc_info=True)

    # Pre-populate form select fields for GET request
    # (obj=task handles simple fields like name, initial_load, create_tables)
    if request.method == 'GET':
        form.source.data = str(task.source_id) # Pre-select dropdown based on ID
        form.destination.data = str(task.destination_id) # Pre-select dropdown based on ID

    # Pass task tables JSON to template for hidden field/JS population
    tables_json_val = json.dumps(task.tables) if task.tables is not None else '[]'
    return render_template('edit_task.html', form=form, task=task, tables_json_val=tables_json_val)

@bp.route('/task/delete/<int:task_id>', methods=['POST'])
def delete_task(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found"}), 404
    if task.status in ['running', 'queued', 'stopping']:
         return jsonify({"success": False, "message": f"Cannot delete task in '{task.status}' state."}), 400
    try:
        db.session.delete(task)
        db.session.commit()
        return jsonify({"success": True, "message": "Task deleted successfully"}), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error deleting task {task_id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"Error deleting task: {str(e)}"}), 500

# --- API Routes ---
@bp.route('/api/endpoints/<int:endpoint_id>/tables', methods=['GET'])
def get_endpoint_tables(endpoint_id):
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint or endpoint.endpoint_type != 'source':
        return jsonify({"error": "Source endpoint not found"}), 404

    connector = None
    try:
        config = build_connector_config(endpoint)
        connector = get_source_connector(endpoint) # Use factory
        connector.connect(config)
        schemas_and_tables = connector.get_schemas_and_tables()
        # Format: list of {'schema': s, 'table': t}
        formatted_tables = []
        for schema, tables in schemas_and_tables.items():
            for table in tables:
                formatted_tables.append({'schema': str(schema), 'table': str(table)})
        formatted_tables.sort(key=lambda x: (x['schema'], x['table']))
        return jsonify(formatted_tables)
    except Exception as e:
        current_app.logger.error(f"Error fetching tables for endpoint {endpoint_id}: {e}", exc_info=True)
        return jsonify({"error": f"Failed to fetch tables: {str(e)}"}), 500
    finally:
        if connector:
            try: connector.disconnect()
            except Exception: pass

@bp.route('/api/source/<int:endpoint_id>/schemas')
def get_source_schemas(endpoint_id):
    # Assuming MetadataService is still relevant/used
    from app.services.metadata_service import MetadataService
    endpoint = db.session.get(Endpoint, endpoint_id)
    if not endpoint: abort(404)
    try:
        endpoint_data = build_connector_config(endpoint) # Use helper
        schemas = MetadataService.get_schemas(endpoint_data) # Check if MetadataService uses dict
        return jsonify(schemas if schemas else {})
    except Exception as e:
        current_app.logger.error(f"Schema fetch error: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@bp.route('/endpoint/test_connection', methods=['POST'])
def test_connection():
    # Assuming test logic is better placed within connector or specific service
    # Reusing MetadataService example for now
    from app.services.metadata_service import MetadataService # Needs review if still used
    form_data = request.form
    endpoint_data = {}
    try:
        endpoint_type_sel = form_data.get('type')
        endpoint_data['type'] = endpoint_type_sel
        endpoint_data['username'] = form_data.get('username')
        endpoint_data['password'] = form_data.get('password')

        # Simplified mapping - assumes form names match Endpoint model more closely now
        type_mapping = {
            'oracle': ['host', 'port', 'service_name'],
            'postgres': ['host', 'port', 'database'],
            'mysql': ['host', 'port', 'database'],
            'bigquery': ['dataset', 'credentials_json']
        }
        for key in type_mapping.get(endpoint_type_sel, []):
            endpoint_data[key] = form_data.get(key) # Get directly if form names match

        # Convert port if present
        if 'port' in endpoint_data and endpoint_data['port']:
            endpoint_data['port'] = int(endpoint_data['port'])
        # Validate JSON if bigquery
        if endpoint_type_sel == 'bigquery' and endpoint_data.get('credentials_json'):
            json.loads(endpoint_data['credentials_json'])

        # Remove None values
        endpoint_data = {k: v for k, v in endpoint_data.items() if v is not None and v != ''}

        # Test connection using a generic approach or specific service
        # Using MetadataService.get_schemas as proxy for connection test
        schemas = MetadataService.get_schemas(endpoint_data)

        return jsonify({'success': True, 'message': 'Connection successful!'})

    except json.JSONDecodeError:
        return jsonify({'success': False, 'message': 'Invalid BigQuery Credentials JSON format.'})
    except ValueError as ve:
         return jsonify({'success': False, 'message': f'Configuration Error: {ve}'})
    except Exception as e:
        current_app.logger.error(f"Connection test failed: {e}", exc_info=True)
        error_msg = str(e).split('\n')[0]
        return jsonify({'success': False, 'message': f'Connection failed: {error_msg}'})

# --- Task Control Routes (Celery) ---
@bp.route('/task/<int:task_id>/run', methods=['POST'])
def run_task(task_id): # Changed function name
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404
    if task.status in ['running', 'queued', 'stopping']:
        return jsonify({"success": False, "message": f"Task already active (status: {task.status})."}), 409
    try:
        async_result = run_replication.apply_async(args=[task.id], kwargs={'perform_initial_load_override': False})
        task.status = 'queued'
        # --- Store Celery Task ID ---
        if hasattr(task, 'celery_task_id'):
            task.celery_task_id = async_result.id  # UNCOMMENT THIS LINE
        else:
            # This else block should ideally not be needed if migration was successful
            current_app.logger.error(f"Task model {task.id} is missing 'celery_task_id' field after migration!")
        db.session.commit()
        current_app.logger.info(f"Task {task.id} queued. Celery ID: {async_result.id}")
        return jsonify({"success": True, "message": f"Task {task.id} queued.", "celery_task_id": async_result.id}), 202
    except Exception as e:
        current_app.logger.error(f"Failed to enqueue task {task.id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Failed to queue task."}), 500

@bp.route('/task/<int:task_id>/reload', methods=['POST'])
def reload_task(task_id): # Changed function name
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404
    if task.status in ['running', 'queued', 'stopping']:
        return jsonify({"success": False, "message": f"Task already active (status: {task.status}). Cannot reload."}), 409
    try:
        async_result = run_replication.apply_async(args=[task.id], kwargs={'perform_initial_load_override': True})
        task.status = 'queued'
        # --- Store Celery Task ID ---
        if hasattr(task, 'celery_task_id'):
            task.celery_task_id = async_result.id  # UNCOMMENT THIS LINE
        else:
            # This else block should ideally not be needed if migration was successful
            current_app.logger.error(f"Task model {task.id} is missing 'celery_task_id' field after migration!")
        db.session.commit()
        current_app.logger.info(f"Task {task.id} queued for reload. Celery ID: {async_result.id}")
        return jsonify(
            {"success": True, "message": f"Task {task.id} reload queued.", "celery_task_id": async_result.id}), 202
    except Exception as e:
        current_app.logger.error(f"Failed to enqueue reload for task {task.id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Failed to queue task reload."}), 500

@bp.route('/task/<int:task_id>/stop', methods=['POST'])
# routes.py - Updated stop_task_api function

@bp.route('/task/<int:task_id>/stop', methods=['POST'])
def stop_task_api(task_id): # Renamed function
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404

    if task.status == 'running' or task.status == 'queued':
         task.status = 'stopping'
         # --- Get Celery Task ID ---
         # Use getattr for safety in case field was somehow missed, though migrate should ensure it exists
         celery_task_id = getattr(task, 'celery_task_id', None) # UNCOMMENTED assignment
         db.session.commit() # Commit status change first
         current_app.logger.info(f"Signalled task {task.id} to stop by setting status to 'stopping'.")

         # --- Add Celery Revoke Logic ---
         if celery_task_id: # UNCOMMENTED Check
             try:
                 # celery_app should already be imported at the top
                 # Send terminate signal to worker process
                 current_app.control.revoke(celery_task_id, terminate=True, signal='SIGTERM') # UNCOMMENTED Revoke
                 current_app.logger.info(f"Sent revoke/terminate signal to Celery task {celery_task_id} for task {task.id}.") # UNCOMMENTED Log
             except Exception as revoke_e:
                 current_app.logger.error(f"Failed to send revoke signal to Celery task {celery_task_id}: {revoke_e}") # UNCOMMENTED Error Log
         else:
              current_app.logger.warning(f"Cannot send revoke signal for task {task.id}: Celery Task ID not found/stored in DB.") # UNCOMMENTED Warning
         # --- End Revoke Logic ---

         return jsonify({"success": True, "message": "Task stop requested."}), 200
    elif task.status == 'stopping':
         return jsonify({"success": True, "message": "Task is already stopping."}), 200
    else:
         return jsonify({"success": False, "message": f"Task status is '{task.status}', cannot stop."}), 400


@bp.route('/task/<int:task_id>/status', methods=['GET'])
def get_task_status_api(task_id): # Renamed function
    task = db.session.get(ReplicationTask, task_id)
    if not task: return jsonify({"success": False, "message": "Task not found."}), 404

    status_data = {
        "success": True, "task_id": task.id, "status": task.status,
        "last_updated": task.last_updated.isoformat() if task.last_updated else None,
        "last_position": task.last_position,
        "metrics": task.metrics or {}
    }
    # --- Query Celery result backend ---
    celery_task_id = getattr(task, 'celery_task_id', None) # Safely get ID (UNCOMMENTED)
    if celery_task_id: # UNCOMMENTED Check
        try:
            # celery_app should already be imported at the top
            async_result = AsyncResult(celery_task_id, app=current_app) # Pass app instance (UNCOMMENTED)
            status_data["celery_status"] = async_result.status # PENDING, STARTED, SUCCESS, FAILURE etc. (UNCOMMENTED)
            if async_result.failed():
                 status_data["celery_error"] = str(async_result.info) # Or async_result.traceback (UNCOMMENTED)
        except Exception as e:
            current_app.logger.warning(f"Could not fetch Celery status for task {task.id} (Celery ID: {celery_task_id}): {e}") # UNCOMMENTED Warning
    # --- End Celery Status Check ---

    return jsonify(status_data), 200


    return jsonify(status_data), 200

# --- Remove other duplicate/commented routes ---