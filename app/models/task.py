from .. import db


class ReplicationTask(db.Model):
    __tablename__ = 'replication_task'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'), nullable=False)
    destination_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'), nullable=False)
    tables = db.Column(db.JSON)
    cdc_type = db.Column(db.String(20))
    cdc_config = db.Column(db.JSON)
    status = db.Column(db.String(20), default='stopped')
    options = db.Column(db.JSON)
    last_position = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=db.func.now())

    # Fixed relationships (removed duplicates)
    source = db.relationship('Endpoint', foreign_keys=[source_id], backref='source_tasks')
    destination = db.relationship('Endpoint', foreign_keys=[destination_id], backref='destination_tasks')

    def __repr__(self):
        return f'<ReplicationTask {self.name}>'