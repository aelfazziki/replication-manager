from app import db
from datetime import datetime


class Endpoint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    type = db.Column(db.String(20), nullable=False)

    # Common fields
    username = db.Column(db.String(50))  # Add this
    password = db.Column(db.String(100))  # Add this

    # Oracle specific fields
    host = db.Column(db.String(120))
    port = db.Column(db.Integer)
    service_name = db.Column(db.String(50))

    # BigQuery specific fields
    dataset = db.Column(db.String(100))
    credentials_json = db.Column(db.Text)

    # MySQL specific fields
    database = db.Column(db.String(100))

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    def __repr__(self):
        return f'<Endpoint {self.name}>'
