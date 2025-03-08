from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    SelectField,
    IntegerField,
    PasswordField,
    TextAreaField,
    SubmitField,
    BooleanField,SelectMultipleField
)
from wtforms.validators import DataRequired, Length


class EndpointForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired()])
    type = SelectField('Type', choices=[
        ('oracle', 'Oracle'),
        ('mysql', 'MySQL'),
        ('bigquery', 'BigQuery')
    ], validators=[DataRequired()])
    host = StringField('Host')
    port = IntegerField('Port')
    service_name = StringField('Service Name')
    database = StringField('Database')
    username = StringField('Username')
    password = PasswordField('Password')
    dataset = StringField('Dataset')
    credentials_json = TextAreaField('Credentials JSON')
    submit = SubmitField('Save')

class TaskForm(FlaskForm):
    name = StringField('Name', validators=[
        DataRequired(),
        Length(min=3, max=50)
    ])
    source = SelectField('Source Endpoint', choices=[], validators=[DataRequired()])
    destination = SelectField('Destination Endpoint', choices=[], validators=[DataRequired()])
    initial_load = BooleanField('Perform Initial Load', default=True)
    create_tables = BooleanField('Create Tables if Missing', default=True)
    submit = SubmitField('Save Task')