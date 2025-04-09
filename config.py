# config.py

import os
from cryptography.fernet import Fernet

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-key')
    # --- Use absolute path for database ---
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL',
                                        'sqlite:///' + os.path.join(basedir, 'app.db'))
    # --- End change ---
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    BIGQUERY_CREDENTIALS = os.getenv('BIGQUERY_CREDENTIALS', 'credentials.json')
    _default_key = Fernet.generate_key().decode()
    ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', _default_key)

    # Celery Configuration (Keep as before)
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//')
    # --- Celery Configuration ---
    # Ensure your broker URL is correct (e.g., RabbitMQ, Redis, etc.)
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'amqp://guest:guest@localhost:5672//')

    # *** ENSURE THIS LINE POINTS TO REDIS ***
    CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    # *** END Ensure ***

    # You can remove or keep CELERY_RESULT_EXTENDED = True, it doesn't hurt Redis
    CELERY_RESULT_EXTENDED = True
    # --- End Change ---
