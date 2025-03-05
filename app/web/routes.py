from flask import Blueprint, render_template, request, redirect, url_for, flash
from app.models import Endpoint, ReplicationTask  # Importer les modules
from app import db  # Importer db depuis le package principal

# Déclarer le blueprint en premier
bp = Blueprint('web', __name__, template_folder='templates')

# Routes
@bp.route('/')
def dashboard():
    page = request.args.get('page', 1, type=int)
    per_page = 10

    tasks = ReplicationTask.query.paginate(page=page, per_page=per_page)
    endpoints = Endpoint.query.paginate(page=page, per_page=per_page)

    return render_template('dashboard.html',
                           tasks=tasks,
                           endpoints=endpoints)

from app.forms import TaskForm, EndpointForm


@bp.route('/task/create', methods=['GET', 'POST'])
def create_task():
    form = TaskForm()
    form.source.choices = [(e.id, e.name) for e in Endpoint.query.filter_by(type='source')]
    form.target.choices = [(e.id, e.name) for e in Endpoint.query.filter_by(type='target')]

    if form.validate_on_submit():
        # Création de la tâche
        return redirect(url_for('web.dashboard'))

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

# Routes pour les endpoints
from flask import render_template
from app.forms import EndpointForm  # Import your form class

# Update the route to pass 'form' to the template
@bp.route('/endpoint/create', methods=['GET', 'POST'])
def create_endpoint():
    form = EndpointForm()  # Instantiate your form
    return render_template('create_endpoint.html', form=form)  # Pass 'form' here

from app.services.connection_tester import test_database_connection


@bp.route('/endpoint/test_connection', methods=['POST'])
def test_connection():
    endpoint_type = request.form.get('type')

    config = {
        'type': endpoint_type,
        'host': request.form.get('host'),
        'port': request.form.get('port'),
        'service_name': request.form.get('service_name'),
        'username': request.form.get('username'),
        'password': request.form.get('password'),
        'dataset': request.form.get('dataset'),
        'credentials_json': request.form.get('credentials_json'),
        'database': request.form.get('database')
    }

    success, message = test_database_connection(config)
    return jsonify({'success': success, 'message': message})

