from app import create_app, db

app = create_app()

# Créer la base si elle n'existe pas
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run()