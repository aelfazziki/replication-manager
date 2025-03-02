from flask import Blueprint, render_template, request, redirect, url_for
from app.models import Endpoint, ReplicationTask
from app import db

bp = Blueprint('web', __name__)

@bp.route('/')
def dashboard():
    """Render the dashboard."""
    tasks = ReplicationTask.query.all()
    return render_template('dashboard.html', tasks=tasks)

@bp.route('/task/<int:task_id>/control', methods=['POST'])
def control_task(task_id):
    """Control a replication task."""
    task = ReplicationTask.query.get_or_404(task_id)
    action = request.form.get('action')
    
    if action == 'start':
        # Start replication
        task.status = 'running'
    elif action == 'stop':
        task.status = 'stopped'
    
    db.session.commit()
    return redirect(url_for('web.dashboard'))