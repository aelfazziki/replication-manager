from app import db

class Endpoint(db.Model):
    """Represents a source or target endpoint."""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    type = db.Column(db.String(20))  # oracle, mysql, bigquery
    config = db.Column(db.JSON)  # Encrypted connection details
    created_at = db.Column(db.DateTime, default=db.func.now())
    updated_at = db.Column(db.DateTime, default=db.func.now(), 
                         onupdate=db.func.now())

    def get_connection(self):
        """Get decrypted connection details."""
        from app.utils.security import SecurityManager
        security = SecurityManager()
        return security.decrypt(self.config)