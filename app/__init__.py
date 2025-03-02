from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from app.utils.security import SecurityManager

# Initialize extensions
db = SQLAlchemy()
migrate = Migrate()
security = SecurityManager()

def create_app():
    """Application factory function."""
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)
    security.init_app(app)

    # Register blueprints
    from app.web.routes import bp as web_bp
    app.register_blueprint(web_bp)

    return app