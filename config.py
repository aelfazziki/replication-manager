# config.py (Updated)
import os
from cryptography.fernet import Fernet
# Import timedelta if you want to configure schedules or timeouts
# from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    # Flask Settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-key')
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'sqlite:///' + os.path.join(basedir, 'app.db'))
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Application Specific Settings
    BIGQUERY_CREDENTIALS = os.getenv('BIGQUERY_CREDENTIALS', 'credentials.json')
    # Generate a key only once if not set, otherwise load existing key
    _default_key = Fernet.generate_key().decode()
    ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', _default_key)

    # --- Celery Configuration ---
    # Use RabbitMQ broker URL (update if you use a different host/user/pass)
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//')
    # Use Redis result backend URL (update if needed)
    CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

    # Optional Celery settings:
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_TIMEZONE = 'UTC' # Recommended to use UTC
    # Example: Ignore results for tasks by default (can be overridden per task)
    # CELERY_TASK_IGNORE_RESULT = True
    # Example: Set a default task time limit (e.g., 1 hour)
    # CELERY_TASK_TIME_LIMIT = 3600
    # Example: Configure task routing or rate limits (more advanced)
    # CELERY_TASK_ROUTES = {'app.tasks.add': 'low-priority'}
    # CELERY_TASK_ANNOTATIONS = {'app.tasks.multiply': {'rate_limit': '10/m'}}

    # Celery Beat schedule (if you plan to use scheduled tasks)
    # CELERY_BEAT_SCHEDULE = {
    #     'periodic-cleanup': {
    #         'task': 'app.tasks.cleanup', # Example task path
    #         'schedule': timedelta(hours=24), # Example: Run daily
    #     },
    # }