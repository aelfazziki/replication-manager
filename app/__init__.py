from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import logging

# Initialiser les extensions AVANT de créer l'app
db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')
    logging.basicConfig(level=logging.INFO)
    app.logger.setLevel(logging.INFO)
    # Initialiser les extensions avec l'app
    db.init_app(app)
    # Register blueprints AFTER db initialization
    with app.app_context():
        from app.web import routes
        app.register_blueprint(routes.bp)

    migrate.init_app(app, db)

    # Importer les routes et modèles APRÈS l'initialisation
#    from app.web import routes
#    app.register_blueprint(routes.bp)

    return app