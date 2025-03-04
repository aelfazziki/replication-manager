from .. import db  # Relative import
from .. import db

class ReplicationTask(db.Model):
    __tablename__ = 'replication_task'  # Explicit table name
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'))
    target_id = db.Column(db.Integer, db.ForeignKey('endpoint.id'))
    tables = db.Column(db.JSON)  # {"include": ["schema.table"], "exclude": [...]}
    cdc_type = db.Column(db.String(20))  # logminer/binlog/timestamp
    cdc_config = db.Column(db.JSON)
    status = db.Column(db.String(20), default='stopped')
    options = db.Column(db.JSON)  # {create_tables: True, ddl_replication: True,...}
    last_position = db.Column(db.JSON)  # SCN/GTID/timestamp
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    source = db.relationship('Endpoint', foreign_keys=[source_id])
    target = db.relationship('Endpoint', foreign_keys=[target_id])

    source = db.relationship('Endpoint', foreign_keys=[source_id], backref='source_tasks')
    target = db.relationship('Endpoint', foreign_keys=[target_id], backref='target_tasks')