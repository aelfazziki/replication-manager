from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify,current_app
from app.models import Endpoint, ReplicationTask
from app import db
from app.forms import TaskForm, EndpointForm
from threading import Thread
import json
from app.replication_worker import run_replication  # Add this line
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
                username=form.username.data,
                password=form.password.data,
                host=form.host.data if form.type.data == 'oracle' else None,
                port=form.port.data if form.type.data == 'oracle' else None,
                service_name=form.service_name.data if form.type.data == 'oracle' else None,
                dataset=form.dataset.data if form.type.data == 'bigquery' else None,
                credentials_json=form.credentials_json.data if form.type.data == 'bigquery' else None,
                database=form.database.data if form.type.data == 'mysql' else None
            )

            db.session.add(new_endpoint)
            db.session.commit()
            flash('Endpoint créé avec succès', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erreur de création: {str(e)}', 'danger')

    return render_template('create_endpoint.html', form=form)



@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()

    # Get all endpoints regardless of type
    all_endpoints = Endpoint.query.all()

    # Populate dropdowns
#    form.source.choices = [(str(e.id), e.name) for e in all_endpoints]
#    form.destination.choices = [(str(e.id), e.name) for e in all_endpoints]
    # In your route
    form.source.choices = [(e.id, e.name) for e in Endpoint.query.all()]
    form.destination.choices = [(e.id, e.name) for e in Endpoint.query.all()]

    if form.validate_on_submit():
        try:
            new_task = ReplicationTask(
                name=form.name.data,
                source_id=int(form.source.data),
                destination_id=int(form.destination.data),  # Matches model
                status='stopped',
                tables={},  # Add default empty values for other required fields
                cdc_config={},
                options={}
            )

            db.session.add(new_task)
            db.session.commit()
            flash('Tâche créée avec succès', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Erreur de création: {str(e)}', 'danger')

    return render_template('create_task.html', form=form)


@bp.route('/task/delete/<int:task_id>')
def delete_task(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    db.session.delete(task)
    db.session.commit()
    flash('Tâche supprimée', 'info')
    return redirect(url_for('web.dashboard'))


@bp.route('/task/control/<int:task_id>/<action>')
def control_task(task_id, action):
    task = ReplicationTask.query.get_or_404(task_id)
    if action == 'start':
        # Start replication worker in background
        task.status = 'running'
        Thread(target=run_replication, args=(task.id,)).start()
        flash('Task started successfully', 'success')
    elif action == 'stop':
        task.status = 'stopped'
        flash('Task stop requested', 'info')
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


# Task editing
@bp.route('/task/edit/<int:task_id>', methods=['GET', 'POST'])
@bp.route('/task/create', methods=['GET', 'POST'])
def edit_task(task_id=None):
    task = ReplicationTask.query.get(task_id) if task_id else None
    form = TaskForm()

    # Populate endpoints
    all_endpoints = Endpoint.query.all()
    form.source.choices = [(str(e.id), e.name) for e in all_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in all_endpoints]

    if form.validate_on_submit():
        try:
            # Get selected tables from hidden input
            selected_tables = json.loads(request.form.get('selected-tables', '[]'))
            if not selected_tables:
                raise ValueError("At least one table must be selected")

            if task:
                # Update existing task
                task.name = form.name.data
                task.source_id = int(form.source.data)
                task.destination_id = int(form.destination.data)
                task.tables = selected_tables
                task.initial_load = form.initial_load.data
                task.create_tables = form.create_tables.data
            else:
                # Create new task
                task = ReplicationTask(
                    name=form.name.data,
                    source_id=int(form.source.data),
                    destination_id=int(form.destination.data),
                    tables=selected_tables,
                    initial_load=form.initial_load.data,
                    create_tables=form.create_tables.data,
                    metrics={},  # Add this line
                    status='stopped'
                )
                db.session.add(task)

            db.session.commit()
            flash('Task saved successfully', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error saving task: {str(e)}', 'danger')
            current_app.logger.error(f"Task save error: {str(e)}", exc_info=True)

    # Prepopulate form for existing task
    if task:
        form.name.data = task.name
        form.source.data = str(task.source_id)
        form.destination.data = str(task.destination_id)
        form.initial_load.data = task.initial_load
        form.create_tables.data = task.create_tables

    return render_template('edit_task.html', form=form, task=task)

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
        schemas = MetadataService.get_schemas(endpoint_id)
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
        # Create temporary endpoint object
        class TempEndpoint:
            def __init__(self, form_data):
                self.type = form_data.get('type')
                self.host = form_data.get('host')
                self.port = form_data.get('port')
                self.service_name = form_data.get('service_name')
                self.username = form_data.get('username')
                self.password = form_data.get('password')
                self.dataset = form_data.get('dataset')
                self.credentials_json = form_data.get('credentials_json')
                self.database = form_data.get('database')

        temp_endpoint = TempEndpoint(form_data)
        schemas = MetadataService.get_schemas(temp_endpoint)

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
    task.status = 'stopped'
    db.session.commit()
    return jsonify({'status': 'success', 'message': 'Task stopping'})

@bp.route('/task/<int:task_id>/metrics')
def get_metrics(task_id):
    task = ReplicationTask.query.get_or_404(task_id)
    return jsonify({
        'status': task.status,
        **(task.metrics or {})  # Handle None case
    })