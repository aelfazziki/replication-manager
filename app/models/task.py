from datetime import datetime, timezone
from app import db # Assuming db is imported from app/__init__.py or similar


class ReplicationTask(db.Model):
    __tablename__ = 'replication_task'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
#    metrics = db.Column(db.JSON)  # Stores latency, counts, etc.
    source_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'), nullable=False)
    destination_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'), nullable=False)
 #   tables = db.Column(db.JSON)
    cdc_type = db.Column(db.String(20))
    cdc_config = db.Column(db.JSON)
    status = db.Column(db.String(20), default='stopped')
    options = db.Column(db.JSON)
    last_position = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_updated = db.Column(db.DateTime,
                             default=lambda: datetime.now(timezone.utc),
                             onupdate=lambda: datetime.now(timezone.utc))

    tables = db.Column(db.JSON)  # Store selected tables
    initial_load = db.Column(db.Boolean, default=False)
    create_tables = db.Column(db.Boolean, default=True)
    replication_mode = db.Column(db.String(20), default='full')  # full/partial
    merge_enabled = db.Column(db.Boolean, default=True)
    # Inside the ReplicationTask class definition:
    metrics = db.Column(db.JSON, default={  # Initialize metrics with default values
        'inserts': 0,
        'updates': 0,
        'deletes': 0,
        'bytes_processed': 0,
        'latency': 0,
        'last_updated': datetime.utcnow().isoformat(),
        'last_position': 0
    })
    celery_task_id = db.Column(db.String(255), nullable=True, index=True)  # Example field


    # Fixed relationships (removed duplicates)
    source = db.relationship('Endpoint', foreign_keys=[source_id], backref='source_tasks')
    destination = db.relationship('Endpoint', foreign_keys=[destination_id], backref='destination_tasks')

    def __repr__(self):
        return f'<ReplicationTask {self.name}>'