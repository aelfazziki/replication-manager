from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()


def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Initialize extensions first
    db.init_app(app)
    migrate.init_app(app, db)

    # Import models AFTER initialization
    from app.models import endpoint, task  # Ensures models are registered
    # Register blueprints
    from app.web.routes import bp as web_bp
    app.register_blueprint(web_bp)  # <- This registers the root route


    return app