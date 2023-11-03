"""pol_info

Revision ID: 51ad91ef1bbf
Revises: 6b43cb33eddc
Create Date: 2023-08-10 23:40:04.325340+00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '51ad91ef1bbf'
down_revision = '6b43cb33eddc'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('part_info', sa.Column('pol', sa.String(length=256), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('part_info', 'pol')
    # ### end Alembic commands ###