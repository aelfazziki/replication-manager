import os
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField
from wtforms.validators import DataRequired, Length
# forms.py
from wtforms.csrf.session import SessionCSRF
class BaseForm(FlaskForm):
    class Meta:
        csrf = True
        csrf_class = SessionCSRF
        csrf_secret = os.getenv('CSRF_SECRET')

class TaskForm(FlaskForm):
    name = StringField('Nom', validators=[
        DataRequired(),
        Length(min=3, max=50)
    ])
    source = SelectField('Source', coerce=int, validators=[DataRequired()])
    target = SelectField('Cible', coerce=int, validators=[DataRequired()])
    cdc_type = SelectField('Type CDC', choices=[
        ('logminer', 'Oracle LogMiner'),
        ('timestamp', 'Colonne de timestamp')
    ])

class EndpointForm(FlaskForm):
    name = StringField('Nom', validators=[
        DataRequired(),
        Length(min=3, max=50)
    ])
    type = SelectField('Type', choices=[
        ('oracle', 'Oracle'),
        ('bigquery', 'BigQuery'),
        ('mysql', 'MySQL')
    ], validators=[DataRequired()])