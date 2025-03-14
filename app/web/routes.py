from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify,current_app
from app.models import Endpoint, ReplicationTask
from app import db
from app.forms import TaskForm, EndpointForm
from threading import Thread
import json
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
    if form.validate_on_submit():
        try:
            new_endpoint = Endpoint(
                name=form.name.data,
                type=form.type.data,
                endpoint_type=form.endpoint_type.data,
                username=form.username.data,
                password=form.password.data,
                host=form.host.data if form.type.data in ['oracle', 'mysql'] else None,
                port=form.port.data if form.type.data in ['oracle', 'mysql'] else None,
                service_name=form.service_name.data if form.type.data == 'oracle' else None,
                dataset=form.dataset.data if form.type.data == 'bigquery' else None,
                credentials_json=form.credentials_json.data if form.type.data == 'bigquery' else None,
                database=form.database.data if form.type.data == 'mysql' else None,
                target_schema=form.target_schema.data if form.endpoint_type.data == 'target' else None
            )
            db.session.add(new_endpoint)
            db.session.commit()
            flash('Endpoint created successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Error creating endpoint: {str(e)}', 'danger')
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
                    'last_updated': datetime.utcnow().isoformat(),
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

@bp.route('/task/control/<int:task_id>/<action>')
def control_task(task_id, action):
    from app.replication_worker import run_replication
    from threading import Thread

    task = ReplicationTask.query.get_or_404(task_id)
    if action == 'start':
        task.status = 'running'
        Thread(target=run_replication, args=(task.id, True, False)).start()  # Initial load
        flash('Task started successfully', 'success')
    elif action == 'reload':
        task.status = 'running'
        Thread(target=run_replication, args=(task.id, False, True)).start()  # Full reload
        flash('Task reload started successfully', 'success')
    elif action == 'resume':
        task.status = 'running'
        Thread(target=run_replication, args=(task.id, False, False)).start()  # Resume from last position
        flash('Task resumed successfully', 'success')
    elif action == 'stop':
        task.status = 'stopped'
        flash('Task stopped successfully', 'info')
    db.session.commit()
    return redirect(url_for('web.dashboard'))

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

    if form.validate_on_submit():
        try:
            form.populate_obj(endpoint)
            db.session.commit()
            flash('Endpoint updated successfully', 'success')
            return redirect(url_for('web.dashboard'))
        except Exception as e:
            db.session.rollback()
            flash(f'Update error: {str(e)}', 'danger')

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

@bp.route('/endpoint/test_connection', methods=['POST'])
def test_connection():
    from app.services.metadata_service import MetadataService
    form_data = request.form
    endpoint_type = form_data.get('type')

    try:
        # Create a dictionary of endpoint data
        endpoint_data = {
            'type': form_data.get('type'),
            'host': form_data.get('host'),
            'port': form_data.get('port'),
            'service_name': form_data.get('service_name'),
            'username': form_data.get('username'),
            'password': form_data.get('password'),
            'dataset': form_data.get('dataset'),
            'credentials_json': form_data.get('credentials_json'),
            'database': form_data.get('database')
        }

        # Use the dictionary directly instead of a TempEndpoint object
        schemas = MetadataService.get_schemas(endpoint_data)

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
        **(task.metrics or {})  # Handle None case
    })
@bp.route('/task/<int:task_id>/reload', methods=['POST'])
def reload_task(task_id):
    from app.replication_worker import run_replication
    from threading import Thread

    task = ReplicationTask.query.get_or_404(task_id)
    if task:
        task.status = 'running'
        task.last_position = None  # Reset last position for full reload
        db.session.commit()
        Thread(target=run_replication, args=(task.id, True, False)).start()
        return jsonify({"success": True, "status": "reloading"}), 200
    else:
        return jsonify({"success": False, "message": "Task not found"}), 404

@bp.route('/task/<int:task_id>/resume', methods=['POST'])
def resume_task(task_id):
    from app.replication_worker import run_replication
    from threading import Thread
    task = ReplicationTask.query.get_or_404(task_id)
    if task and task.last_position:
        task.status = 'running'
        db.session.commit()
        Thread(target=run_replication, args=(task.id, False, True)).start()
        return jsonify({"success": True, "status": "resuming"}), 200
    else:
        return jsonify({"success": False, "message": "Task not found or no last position"}), 404