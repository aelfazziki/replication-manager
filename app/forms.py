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
    # Basic Information
    name = StringField('Name', validators=[DataRequired()])
    type = SelectField('Database Type', choices=[
        ('postgres', 'PostgreSQL'),
        ('oracle', 'Oracle'),
        ('mysql', 'MySQL'),
        ('bigquery', 'BigQuery')
    ], validators=[DataRequired()])
    endpoint_type = SelectField('Endpoint Role', choices=[
        ('source', 'Source'),
        ('target', 'Target')
    ], validators=[DataRequired()])

    # Credentials
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[Optional()])

    # PostgreSQL Fields
    postgres_host = StringField('Host', validators=[Optional()])
    postgres_port = IntegerField('Port', validators=[Optional(), NumberRange(min=1, max=65535)])
    postgres_database = StringField('Database', validators=[Optional()])

    # Oracle Fields
    oracle_host = StringField('Host', validators=[Optional()])
    oracle_port = IntegerField('Port', validators=[Optional(), NumberRange(min=1, max=65535)])
    oracle_service_name = StringField('Service Name', validators=[Optional()])

    # MySQL Fields
    mysql_host = StringField('Host', validators=[Optional()])
    mysql_port = IntegerField('Port', validators=[Optional(), NumberRange(min=1, max=65535)])
    mysql_database = StringField('Database', validators=[Optional()])

    # BigQuery Fields
    dataset = StringField('Dataset', validators=[Optional()])
    credentials_json = TextAreaField('Service Account JSON', validators=[Optional()])

    # Target Schema (for target endpoints)
    target_schema = StringField('Target Schema', validators=[Optional()])

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