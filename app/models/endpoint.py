from app import db
from datetime import datetime


class Endpoint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(20), nullable=False)  # oracle, mysql, bigquery
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(100))  # For oracle/mysql
    port = db.Column(db.Integer)  # For oracle/mysql
    service_name = db.Column(db.String(100))  # For oracle
    dataset = db.Column(db.String(100))  # For bigquery
    credentials_json = db.Column(db.Text)  # For bigquery
    database = db.Column(db.String(100))  # For mysql
    created_at = db.Column(db.DateTime, default=db.func.now())
    target_schema = db.Column(db.String(100))  # New field for target schema
    endpoint_type = db.Column(db.String(20), nullable=False, default='source')  # source or target
    def __repr__(self):
        return f'<Endpoint {self.name}>'
