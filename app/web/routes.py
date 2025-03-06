from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models import Endpoint, ReplicationTask
from app import db
from app.forms import TaskForm, EndpointForm
from app.services.connection_tester import test_database_connection

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


@bp.route('/endpoint/test_connection', methods=['POST'])
def test_connection():
    form_data = request.form
    config = {
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

    success, message = test_database_connection(config)
    return jsonify({'success': success, 'message': message})


@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()

    # Get all endpoints regardless of type
    all_endpoints = Endpoint.query.all()

    # Populate dropdowns
    form.source.choices = [(str(e.id), e.name) for e in all_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in all_endpoints]

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
    if action in ['start', 'stop', 'pause']:
        task.status = action
        db.session.commit()
        flash(f'Tâche {action}ée', 'success')
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
@bp.route('/task/create', methods=['GET', 'POST'])
@bp.route('/task/edit/<int:task_id>', methods=['GET', 'POST'])
def edit_task(task_id=None):
    task = ReplicationTask.query.get_or_404(task_id) if task_id else None
    form = TaskForm(obj=task)

    # Populate endpoints
    all_endpoints = Endpoint.query.all()
    form.source.choices = [(str(e.id), e.name) for e in all_endpoints]
    form.destination.choices = [(str(e.id), e.name) for e in all_endpoints]

    if form.validate_on_submit():
        try:
            # Convert comma-separated tables to list
            tables = [t.strip() for t in form.tables.data.split(',') if t.strip()]

            if task:
                form.populate_obj(task)
                task.tables = tables
            else:
                task = ReplicationTask(
                    name=form.name.data,
                    source_id=int(form.source.data),
                    destination_id=int(form.destination.data),
                    tables=tables,
                    initial_load=form.initial_load.data,
                    create_tables=form.create_tables.data,
                    replication_mode=form.replication_mode.data,
                    status='stopped'
                )
                db.session.add(task)

            db.session.commit()
            flash('Task saved successfully', 'success')
            return redirect(url_for('web.dashboard'))

        except Exception as e:
            db.session.rollback()
            flash(f'Error saving task: {str(e)}', 'danger')

    # Prepopulate tables list
    if task and task.tables:
        form.tables.data = ', '.join(task.tables)

    return render_template('edit_task.html', form=form, task=task)