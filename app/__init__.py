# app/__init__.py (Updated)

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import logging

# --- Import Celery app instance and init function ---
# Adjust path if your celery file is named differently or located elsewhere
from .celery import celery_app, init_celery

# Initialiser les extensions AVANT de créer l'app
db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config') # Make sure config.Config sets CELERY_BROKER_URL etc. if needed

    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)
    # Inside create_app() in app/__init__.py, after app.config.from_object(...):
    app.logger.info(f"Connecting to Database: {app.config['SQLALCHEMY_DATABASE_URI']}")

    # Initialiser les extensions avec l'app
    db.init_app(app)
    migrate.init_app(app, db)

    # --- Initialize Celery ---
    # This updates celery_app.conf from Flask config and sets up context task
    init_celery(app)

    # --- Register blueprints AFTER Celery and DB initialization ---
    # No need for 'with app.app_context():' just for registration
    from app.web import routes
    app.register_blueprint(routes.bp)

    # Ensure models are imported so Flask-Migrate sees them (often done implicitly via routes/views)
    # from . import models # Uncomment if models aren't imported elsewhere

    app.logger.info("Flask app created and configured.")
    return app