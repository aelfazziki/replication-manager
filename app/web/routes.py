from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from app.models import Endpoint, ReplicationTask # Assuming models are in app.models
from app import db # Assuming db instance is created in app package
from app.forms import TaskForm, EndpointForm # Assuming forms are in app.forms
from datetime import datetime, timezone
import json # Added missing import
# --- Import the Celery task ---
# Adjust the import path based on where your Celery task is defined
from app.replication_worker import run_replication, build_connector_config, get_source_connector  # Or maybe tasks.py
def run_replication(*args, **kwargs):
            # Dummy task function
            class DummyAsyncResult:
                id = "DUMMY_TASK_ID_IMPORT_FAILED"
                def delay(*args, **kwargs):
                    print("ERROR: run_replication task not imported correctly.")
                    return DummyAsyncResult()
            return DummyAsyncResult()

#from app.replication_worker import run_replication  # Add this line

bp = Blueprint('web', __name__, template_folder='templates')


@bp.route('/')
def dashboard():
    page = request.args.get('page', 1, type=int)
    per_page = 10

    # Add explicit ordering and remove pagination for testing
    tasks = ReplicationTask.query.order_by(ReplicationTask.created_at.desc()).all()
    endpoints = Endpoint.query.order_by(Endpoint.created_at.desc()).all()

    return render_template('dashboard.html',
                           tasks=tasks,
                           endpoints=endpoints)


@bp.route('/endpoint/create', methods=['GET', 'POST'])
def create_endpoint():
    form = EndpointForm()
    current_app.logger.info(f"Before Form data validation: {form.data}")
    if form.validate_on_submit():
        try:
            # Log form data for debugging
            current_app.logger.info(f"Form data received: {form.data}")
            db_type = form.type.data
            host = form.postgres_host.data if db_type == 'postgres' else form.oracle_host.data
            port = form.postgres_port.data if db_type == 'postgres' else form.oracle_port.data
            database = form.postgres_database.data if db_type == 'postgres' else None
            service_name = form.oracle_service_name.data if db_type == 'oracle' else None

            new_endpoint = Endpoint(
                name=form.name.data,
                type=db_type,
                endpoint_type=form.endpoint_type.data,
                username=form.username.data,
                password=form.password.data,
                host=host,
                port=port,
                service_name=service_name,
                dataset=form.dataset.data if form.type.data == 'bigquery' else None,
                credentials_json=form.credentials_json.data if form.type.data == 'bigquery' else None,
                database=database,
                target_schema=form.target_schema.data if form.endpoint_type.data == 'target' else None
            )
            db.session.add(new_endpoint)
            current_app.logger.info("Endpoint added to session. Attempting to commit...")
            db.session.commit()
            current_app.logger.info("Endpoint successfully committed to the database.")
            flash('Endpoint created successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating endpoint: {str(e)}', 'danger')
            current_app.logger.error(f"Error creating endpoint: {str(e)}", exc_info=True)
    else:
        # Log form validation errors
        current_app.logger.info(f"Form validation errors: {form.data}")
        current_app.logger.error(f"Form validation errors: {form.errors}")
        return render_template('create_endpoint.html', form=form)

@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()

    # Filter endpoints by endpoint_type
    source_endpoints = Endpoint.query.filter_by(endpoint_type='source').all()
    target_endpoints = Endpoint.query.filter_by(endpoint_type='target').all()

    # Populate dropdowns with filtered endpoints
    form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]

    if form.validate_on_submit():
        try:
            new_task = ReplicationTask(
                name=form.name.data,
                source_id=int(form.source.data),
                destination_id=int(form.destination.data),
                status='stopped',
                tables={},  # Add default empty values for other required fields
                cdc_config={},
                options={},
                metrics={  # Initialize metrics with default values
                    'inserts': 0,
                    'updates': 0,
                    'deletes': 0,
                    'bytes_processed': 0,
                    'latency': 0,
                    'last_updated': datetime.now(timezone.utc).isoformat(),
                    'last_position': 0
                }
            )

            db.session.add(new_task)
            db.session.commit()
            flash('Task created successfully', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error creating task: {str(e)}', 'danger')

    return render_template('create_task.html', form=form)

@bp.route('/task/edit/<int:task_id>', methods=['GET', 'POST'])
def edit_task(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    print("Task tables:", task.tables)  # Debugging

    form = TaskForm()

    # Filter endpoints by endpoint_type
    source_endpoints = Endpoint.query.filter_by(endpoint_type='source').all()
    target_endpoints = Endpoint.query.filter_by(endpoint_type='target').all()

    # Populate dropdowns with filtered endpoints
    form.source.choices = [(str(e.id), e.name) for e in source_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in target_endpoints]

    if form.validate_on_submit():
        try:
            selected_tables = request.form.get('selected-tables', '[]')

            # Validate JSON format
            if not selected_tables.strip():
                selected_tables = '[]'

            try:
                task.tables = json.loads(selected_tables)
            except json.JSONDecodeError as e:
                current_app.logger.error(f"JSON decode error: {str(e)}")
                task.tables = []

            # Update other task properties
            task.name = form.name.data
            task.source_id = int(form.source.data)
            task.destination_id = int(form.destination.data)
            task.initial_load = form.initial_load.data
            task.create_tables = form.create_tables.data

            db.session.commit()
            flash('Task updated successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error updating task: {str(e)}', 'danger')
            current_app.logger.error(f"Task update error: {str(e)}", exc_info=True)

    # Pre-populate form for existing task
    if request.method == 'GET':
        form.name.data = task.name
        form.source.data = str(task.source_id)
        form.destination.data = str(task.destination_id)
        form.initial_load.data = task.initial_load
        form.create_tables.data = task.create_tables

    return render_template('edit_task.html', form=form, task=task)

@bp.route('/task/delete/<int:task_id>', methods=['POST'])
def delete_task(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    try:
        db.session.delete(task)
        db.session.commit()
        return jsonify({"success": True, "message": "Task deleted successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

# Remove the top-level import
# from app.replication_worker import run_replication


# Endpoint deletion
@bp.route('/endpoint/delete/<int:endpoint_id>')
def delete_endpoint(endpoint_id):
    endpoint = Endpoint.query.get_or_404(endpoint_id)
    db.session.delete(endpoint)
    db.session.commit()
    flash('Endpoint deleted successfully', 'success')
    return redirect(url_for('web.dashboard'))


# Endpoint editing
@bp.route('/endpoint/edit/<int:endpoint_id>', methods=['GET', 'POST'])
def edit_endpoint(endpoint_id):
    endpoint = Endpoint.query.get_or_404(endpoint_id)
    form = EndpointForm(obj=endpoint)

    # Manually set the type since the field is disabled
    form.type.data = endpoint.type

    if request.method == 'GET':
        # Pre-populate type-specific fields
        if endpoint.type == 'postgres':
            form.postgres_host.data = endpoint.host
            form.postgres_port.data = endpoint.port
            form.postgres_database.data = endpoint.database
        elif endpoint.type == 'oracle':
            form.oracle_host.data = endpoint.host
            form.oracle_port.data = endpoint.port
            form.oracle_service_name.data = endpoint.service_name

    if form.validate_on_submit():
        try:
            # Update common fields
            endpoint.name = form.name.data
            endpoint.endpoint_type = form.endpoint_type.data
            endpoint.username = form.username.data
            endpoint.password = form.password.data
            endpoint.target_schema = form.target_schema.data

            # Update type-specific fields
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

            db.session.commit()
            flash('Endpoint updated successfully', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error updating endpoint: {str(e)}', 'danger')
            current_app.logger.error(f"Endpoint update error: {str(e)}", exc_info=True)

    return render_template('edit_endpoint.html', form=form, endpoint=endpoint)

@bp.route('/api/source/<int:endpoint_id>/tables')
def get_source_tables(endpoint_id):
    from app.services.metadata_service import MetadataService
    try:
        tables = MetadataService.get_tables(endpoint_id)
        return jsonify({'tables': sorted(tables)})
    except Exception as e:
        current_app.logger.error(f"Table fetch error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@bp.route('/api/source/<int:endpoint_id>/schemas')
def get_source_schemas(endpoint_id):
    from app.services.metadata_service import MetadataService
    try:
        # Fetch the endpoint object from the database
        endpoint = Endpoint.query.get_or_404(endpoint_id)
        # Convert the endpoint object to a dictionary
        endpoint_data = {
            'type': endpoint.type,
            'host': endpoint.host,
            'port': endpoint.port,
            'username': endpoint.username,
            'password': endpoint.password,
            'service_name': endpoint.service_name,
            'dataset': endpoint.dataset,
            'credentials_json': endpoint.credentials_json,
            'database': endpoint.database
        }
        # Pass the dictionary to MetadataService.get_schemas
        schemas = MetadataService.get_schemas(endpoint_data)
        return jsonify(schemas)
    except Exception as e:
        current_app.logger.error(f"Schema fetch error: {str(e)}")
        return jsonify({'error': str(e)}), 500
# --- Task Control Routes (Updated for Celery) ---

@bp.route('/task/<int:task_id>/run', methods=['POST'])
def run_task_api(task_id): # Changed function name slightly
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    # Optional: Add logic to prevent starting if already running/queued
    # if task.status == 'running' or task is already queued:
    #     return jsonify({"success": False, "message": "Task is already running or queued."}), 409

    # Get start_datetime from request if provided (used by your JS)
    start_datetime_str = request.json.get('start_datetime')
    # Note: The Celery task 'run_replication' needs to handle this parameter if used.
    # The current 'run_replication' signature doesn't use start_datetime. Modify if needed.

    try:
        # Enqueue the task using Celery's delay() method
        # Pass necessary arguments defined in the task function signature
        async_result = run_replication.delay(task.id, perform_initial_load_override=False)
        current_app.logger.info(f"Task {task.id} queued for execution. Celery task ID: {async_result.id}")

        # Optionally update task status to 'queued' or similar
        task.status = 'queued' # Or keep as 'stopped' until worker picks it up
        db.session.commit()

        return jsonify({"success": True, "message": f"Task {task.id} queued successfully.", "celery_task_id": async_result.id}), 202 # 202 Accepted
    except Exception as e:
        current_app.logger.error(f"Failed to enqueue task {task.id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Failed to queue task."}), 500


@bp.route('/task/<int:task_id>/reload', methods=['POST'])
def reload_task_api(task_id): # Changed function name slightly
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    try:
        # Enqueue the task with initial load override set to True
        async_result = run_replication.delay(task.id, perform_initial_load_override=True)
        current_app.logger.info(f"Task {task.id} queued for reload. Celery task ID: {async_result.id}")

        task.status = 'queued' # Or keep as 'stopped'
        db.session.commit()

        return jsonify({"success": True, "message": f"Task {task.id} reload queued successfully.", "celery_task_id": async_result.id}), 202 # 202 Accepted
    except Exception as e:
        current_app.logger.error(f"Failed to enqueue task {task.id} for reload: {e}", exc_info=True)
        return jsonify({"success": False, "message": "Failed to queue task reload."}), 500


# --- Placeholder for Stopping a Task (Requires Celery Control/Revoke API) ---
@bp.route('/task/<int:task_id>/stop', methods=['POST'])
def stop_task_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    # Implementation requires interacting with Celery:
    # 1. Find the Celery task ID associated with the running task (needs to be stored).
    # 2. Use `celery_app.control.revoke(celery_task_id, terminate=True, signal='SIGTERM')`.
    # 3. Update the task status in your database (`task.status = 'stopped'`).
    # This needs more complex state management.

    # Simple approach: Just update DB status, Celery task needs to check this status periodically.
    if task.status == 'running' or task.status == 'queued':
         task.status = 'stopping' # Signal the worker to stop
         db.session.commit()
         current_app.logger.info(f"Attempting to signal task {task.id} to stop.")
         return jsonify({"success": True, "message": "Task stop signal sent. Worker will stop on next check."}), 200
    else:
         return jsonify({"success": False, "message": f"Task status is '{task.status}', cannot stop."}), 400

# --- Placeholder for Task Status API (Requires Celery Result Backend) ---
@bp.route('/task/<int:task_id>/status', methods=['GET'])
def get_task_status_api(task_id):
    task = db.session.get(ReplicationTask, task_id)
    if not task:
        return jsonify({"success": False, "message": "Task not found."}), 404

    # Implementation requires:
    # 1. Storing the Celery task ID when the task is started.
    # 2. Using `run_replication.AsyncResult(celery_task_id)` to get status from Celery backend.
    # 3. Combining Celery status with your application's DB status (`task.status`).

    # Simple approach: Just return DB status
    return jsonify({
        "success": True,
        "task_id": task.id,
        "status": task.status, # Status from your application DB
        "last_updated": task.last_updated.isoformat() if task.last_updated else None,
        "last_position": task.last_position
        # Add metrics here too if needed
    }), 200


@bp.route('/endpoint/test_connection', methods=['POST'])
def test_connection():
    from app.services.metadata_service import MetadataService
    form_data = request.form
    endpoint_type = form_data.get('type')
    current_app.logger.info(f"Form data: {form_data}")
    try:
        # Create a dictionary of endpoint data
        endpoint_data = {
            'type': endpoint_type,
            'host': form_data.get('host'),
            'port': form_data.get('port'),
            'service_name': form_data.get('service_name'),
            'username': form_data.get('username'),
            'password': form_data.get('password'),
            'dataset': form_data.get('dataset'),
            'credentials_json': form_data.get('credentials_json'),
            'database': form_data.get('database')
        }
        # Base fields common to all types
        endpoint_data = {
            'type': endpoint_type,
            'username': form_data.get('username'),
            'password': form_data.get('password'),
            'endpoint_type': form_data.get('endpoint_type'),
            'target_schema': form_data.get('target_schema')
        }

        # Type-specific field mapping
        type_mapping = {
            'oracle': {
                'host': 'oracle_host',
                'port': 'oracle_port',
                'service_name': 'oracle_service_name'
            },
            'postgres': {
                'host': 'postgres_host',
                'port': 'postgres_port',
                'database': 'postgres_database'
            },
            'mysql': {
                'host': 'mysql_host',  # Add these fields to your form if missing
                'port': 'mysql_port',
                'database': 'mysql_database'
            },
            'bigquery': {
                'dataset': 'dataset',
                'credentials_json': 'credentials_json'
            }
        }

        # Add type-specific fields
        if endpoint_type in type_mapping:
            for key, form_field in type_mapping[endpoint_type].items():
                endpoint_data[key] = form_data.get(form_field)

        # Handle empty values for numeric fields
        if endpoint_data.get('port'):
            try:
                endpoint_data['port'] = int(endpoint_data['port'])
            except ValueError:
                endpoint_data['port'] = None

        # Now use endpoint_data for your database operations
        print(endpoint_data)  # Debug output

        current_app.logger.info(f"endpoint_data : {endpoint_data}")

        # Use the dictionary directly instead of a TempEndpoint object
        schemas = MetadataService.get_schemas(endpoint_data)
        current_app.logger.info(f"schemas : {schemas}")

        if not schemas:
            return jsonify({'success': False, 'message': 'Connection successful but no schemas found'})

        return jsonify({
            'success': True,
            'message': f'Connection successful. Found {len(schemas)} schemas',
            'schemas': list(schemas.keys())[:5]  # Return first 5 schemas as sample
        })
    except Exception as e:
        error_msg = str(e).split('\n')[0]
        return jsonify({'success': False, 'message': error_msg})

# routes.py
@bp.route('/task/<int:task_id>/stop', methods=['POST'])
def stop_task(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    if task:
        # Update the task status to 'stopped'
        task.status = 'stopped'
        db.session.commit()
        return jsonify({"success": True, "status": "stopped"}), 200
    else:
        return jsonify({"success": False, "message": "Task not found"}), 404

@bp.route('/task/<int:task_id>/metrics')
def get_metrics(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    return jsonify({
        'status': task.status,
        'metrics': task.metrics or {}
    })\


#@bp.route('/task/<int:task_id>/run', methods=['POST'])
#def run_task(task_id):
#    from app.replication_worker import run_replication
#    from threading import Thread
#
#    task = ReplicationTask.query.get_or_404(task_id)
#    start_datetime = request.json.get('start_datetime')
#    Thread(target=run_replication, args=(task.id, False, False, start_datetime)).start()
#    return jsonify({"success": True, "message": "Task started successfully."}), 200

@bp.route('/task/<int:task_id>/resume', methods=['POST'])
def resume_task(task_id):
    from app.replication_worker import run_replication
    from threading import Thread

    task = ReplicationTask.query.get_or_404(task_id)
    Thread(target=run_replication, args=(task.id, False, False)).start()
    return jsonify({"success": True, "message": "Task resumed successfully."}), 200

#@bp.route('/task/<int:task_id>/reload', methods=['POST'])
#def reload_task(task_id):
#    from app.replication_worker import run_replication
#    from threading import Thread
#
#    task = ReplicationTask.query.get_or_404(task_id)
#    Thread(target=run_replication, args=(task.id, True, True)).start()
#    return jsonify({"success": True, "message": "Task reload started successfully."}), 200

@bp.route('/task/<int:task_id>/run', methods=['POST'])
def run_task_endpoint(task_id): # Renamed endpoint function for clarity
    task = ReplicationTask.query.get_or_404(task_id)
    # Optional: Get parameters if needed by the task
    # start_datetime = request.json.get('start_datetime')

    # Enqueue the task
    run_replication.delay(task.id) # Pass necessary arguments

    return jsonify({"success": True, "message": f"Task {task.id} queued for execution."}), 202 # 202 Accepted

@bp.route('/task/<int:task_id>/reload', methods=['POST'])
def reload_task_endpoint(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    # Enqueue the task with initial load override
    run_replication.delay(task.id, perform_initial_load_override=True)
    return jsonify({"success": True, "message": f"Task {task.id} reload queued."}), 202


# --- API endpoint to fetch schemas/tables ---
# (Keep or modify based on task-builder.js needs)
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
        # Format for UI (example: list of {'schema': s, 'table': t})
        formatted_tables = []
        for schema, tables in schemas_and_tables.items():
            for table in tables:
                formatted_tables.append({'schema': schema, 'table': table})
        return jsonify(formatted_tables)
    except Exception as e:
        current_app.logger.error(f"Error fetching tables for endpoint {endpoint_id}: {e}", exc_info=True)
        return jsonify({"error": f"Failed to fetch tables: {e}"}), 500
    finally:
        if connector:
            try:
                connector.disconnect()
            except Exception as e:
                 current_app.logger.error(f"Error disconnecting after fetching tables: {e}", exc_info=True)

@bp.route('/task/control/<int:task_id>/<action>')
def control_task(task_id, action):
    task = ReplicationTask.query.get_or_404(task_id)
    if action == 'stop':
        task.status = 'stopped'
        db.session.commit()
        flash('Task stopped successfully', 'info')
    return redirect(url_for('web.dashboard'))