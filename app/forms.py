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
    type = SelectField('Type', choices=[
        ('oracle', 'Oracle'),
        ('bigquery', 'BigQuery'),
        ('mysql', 'MySQL')
    ], validators=[DataRequired()])

    # Common fields
    username = StringField('Utilisateur')
    password = PasswordField('Mot de passe')

    # Oracle specific
    host = StringField('Hôte')
    port = IntegerField('Port', default=1521)
    service_name = StringField('Nom du service')

    # BigQuery specific
    dataset = StringField('Dataset')
    credentials_json = TextAreaField('Credentials JSON')

    # MySQL specific
    database = StringField('Base de données')

    test_connection = SubmitField('Tester la connexion')
    submit = SubmitField('Create Endpoint')


class TaskForm(FlaskForm):
    name = StringField('Nom', validators=[
        DataRequired(),
        Length(min=3, max=50)
    ])
    source = SelectField('Source Endpoint', choices=[])
    destination = SelectField('Destination Endpoint', choices=[])
    submit = SubmitField('Create Task')