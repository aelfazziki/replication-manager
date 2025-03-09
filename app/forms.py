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
    type = SelectField('Type', choices=[('oracle', 'Oracle'), ('mysql', 'MySQL'), ('bigquery', 'BigQuery')])
    endpoint_type = SelectField('Endpoint Type', choices=[('source', 'Source'), ('target', 'Target')], default='source')
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    host = StringField('Host')  # For oracle/mysql
    port = IntegerField('Port')  # For oracle/mysql
    service_name = StringField('Service Name')  # For oracle
    dataset = StringField('Dataset')  # For bigquery
    credentials_json = TextAreaField('Credentials JSON')  # For bigquery
    database = StringField('Database')  # For mysql
    target_schema = StringField('Target Schema')  # New field for target schema

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