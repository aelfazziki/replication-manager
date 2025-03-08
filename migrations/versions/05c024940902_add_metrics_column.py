"""Add metrics column

Revision ID: 05c024940902
Revises: 80f1c858d253
Create Date: 2025-03-08 00:15:20.368223

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '05c024940902'
down_revision = '80f1c858d253'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('replication_task', schema=None) as batch_op:
        batch_op.add_column(sa.Column('metrics', sa.JSON(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('replication_task', schema=None) as batch_op:
        batch_op.drop_column('metrics')

    # ### end Alembic commands ###
