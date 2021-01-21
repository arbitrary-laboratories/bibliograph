"""Add account to query

Revision ID: e9e8251d6a5e
Revises: 98611031f0b9
Create Date: 2021-01-21 01:37:47.967206

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e9e8251d6a5e'
down_revision = '98611031f0b9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('query_info', sa.Column('account', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('query_info', 'account')
    # ### end Alembic commands ###
