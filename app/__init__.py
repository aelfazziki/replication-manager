from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

# Initialiser les extensions AVANT de créer l'app
db = SQLAlchemy()
migrate = Migrate()

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Initialiser les extensions avec l'app
    db.init_app(app)
    migrate.init_app(app, db)

    # Importer les routes et modèles APRÈS l'initialisation
    from app.web import routes
    app.register_blueprint(routes.bp)

    return app