# config.py
import os
from cryptography.fernet import Fernet  # <-- Add this import

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-key')
    SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    BIGQUERY_CREDENTIALS = os.getenv('BIGQUERY_CREDENTIALS', 'credentials.json')
    ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', Fernet.generate_key().decode())  # Now works