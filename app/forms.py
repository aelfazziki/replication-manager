from wtforms import StringField, IntegerField, PasswordField, validators
from flask_wtf import FlaskForm  # <-- Ajouter cette ligne
from wtforms import StringField, SelectField, TextAreaField, SubmitField
from wtforms.validators import DataRequired, Length

from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    SelectField,
    IntegerField,
    PasswordField,
    TextAreaField,
    SubmitField
)
from wtforms.validators import DataRequired, Length

class EndpointForm(FlaskForm):
    name = StringField('Nom', validators=[DataRequired(), Length(max=100)])
    # Required submit field
    submit = SubmitField('Create Endpoint')
    type = SelectField('Type', choices=[
        ('oracle', 'Oracle'),
        ('bigquery', 'BigQuery'),
        ('mysql', 'MySQL')
    ], validators=[DataRequired()])

    # Champs communs
    username = StringField('Utilisateur')
    password = PasswordField('Mot de passe')

    # Oracle spécifique
    host = StringField('Hôte')
    port = IntegerField('Port', default=1521)
    service_name = StringField('Nom du service')

    # BigQuery spécifique
    dataset = StringField('Dataset')
    credentials_json = TextAreaField('Credentials JSON')

    # MySQL spécifique
    database = StringField('Base de données')

    test_connection = SubmitField('Tester la connexion')

class TaskForm(FlaskForm):
    name = StringField('Nom', validators=[
        DataRequired(),
        Length(min=3, max=50)
    ])
