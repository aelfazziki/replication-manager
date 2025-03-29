# app/celery.py (or tasks.py) - Modified

from celery import Celery
from flask import Flask

# --- REMOVE broker and backend arguments here ---
# They will be picked up from the Flask config via init_celery
celery_app = Celery(__name__)
# --- End Removal ---


def init_celery(app: Flask):
    """Initialize Celery, binding it to the Flask app context."""
    # Update Celery config from Flask app config (reads CELERY_BROKER_URL etc.)
    celery_app.conf.update(app.config)

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    return celery_app