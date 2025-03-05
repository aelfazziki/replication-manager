from .. import db


class Endpoint(db.Model):
    __tablename__ = 'endpoint'  # Spécifier explicitement le nom de la table

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    type = db.Column(db.String(20))
    config = db.Column(db.JSON)
    created_at = db.Column(db.DateTime, default=db.func.now())
    updated_at = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

    def __repr__(self):
        return f'<Endpoint {self.name}>'