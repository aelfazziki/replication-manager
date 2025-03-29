from celery import Celery
from flask import Flask

# Minimal configuration example using Redis
celery_app = Celery(__name__,
                  broker='amqp://guest:guest@localhost:5672//', # <-- CHANGE THIS LINE
                  backend='redis://localhost:6379/0') # You can keep Redis for backend OR change it too if needed

def init_celery(app: Flask):
    """Initialize Celery, binding it to the Flask app context."""
    celery_app.conf.update(app.config)

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    # If using app factory: app.extensions['celery'] = celery_app
    return celery_app

# You might configure Celery via Flask config:
# app.config.update(
#     CELERY_BROKER_URL='redis://localhost:6379/0',
#     CELERY_RESULT_BACKEND='redis://localhost:6379/0'
# )
# celery = init_celery(app) # Assuming 'app' is your Flask instance