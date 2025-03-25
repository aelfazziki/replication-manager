from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    SelectField,
    IntegerField,
    PasswordField,
    TextAreaField,
    SubmitField,
    BooleanField,
    SelectMultipleField
)
from wtforms.validators import DataRequired, Length, Optional, NumberRange  # Added missing imports


class EndpointForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired()])
    type = SelectField('Type', choices=[
        ('oracle', 'Oracle'),
        ('mysql', 'MySQL'),
        ('bigquery', 'BigQuery'),
        ('postgres', 'PostgreSQL')
    ], validators=[DataRequired()])
    endpoint_type = SelectField('Endpoint Type', choices=[
        ('source', 'Source'),
        ('target', 'Target')
    ], validators=[DataRequired()])
    username = StringField('Username', validators=[DataRequired()])
    password = StringField('Password', validators=[DataRequired()])
    target_schema = StringField('Target Schema')

    # PostgreSQL-specific fields
    postgres_host = StringField('PostgreSQL Host')
    postgres_port = IntegerField('PostgreSQL Port', validators=[
        Optional(),
        NumberRange(min=1, max=65535)
    ])
    postgres_database = StringField('PostgreSQL Database')

    # Oracle-specific fields
    oracle_host = StringField('Oracle Host')
    oracle_port = IntegerField('Oracle Port', validators=[
        Optional(),
        NumberRange(min=1, max=65535)
    ])
    oracle_service_name = StringField('Oracle Service Name')

    # Other database type fields
    dataset = StringField('BigQuery Dataset')
    credentials_json = TextAreaField('BigQuery Credentials JSON')
    database = StringField('MySQL Database')

    def validate(self, extra_validators=None):
        # First run default validation
        if not super().validate():
            return False

        # Custom validation based on selected type
        if self.type.data == 'oracle':
            if not self.oracle_port.data:
                self.oracle_port.errors.append('Port is required for Oracle')
                return False
        elif self.type.data == 'postgres':
            if not self.postgres_port.data:
                self.postgres_port.errors.append('Port is required for PostgreSQL')
                return False

        return True


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