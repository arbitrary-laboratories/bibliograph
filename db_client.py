from alexandria.data_models import (
    Organization,
    Table,
    Column,
    db,
)

from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData

from alexandria.utils import get_bq_gateway


class DbClient(object):

    def org_init(self, org_id, init=True):
        """ Returns all tables for an org """

        q = db.session.query(Table).filter_by(org_id=org_id)
        return [(i.table_id,
                 i.name,
                 i.description,
                 i.pii_flag,
                 i.warehouse_full_table_id) for i in q.all()]

    def table_load(self, table_id):
        """ returns table for a given table_id """
        q = db.session.query(Table).filter_by(table_id=table_id)
        return q.all()

    def columns_load(self, table_id):
        """ returns all column metadata for a given table """

        q = db.session.query(Column).filter_by(table_id=table_id)
        return results.all()

    def edit_table_pii_flag(self, table_id, pii_flag):
        """ update the pii flag for a table, returns table"""
        q = db.session.query(Table).filter_by(table_id=table_id)
        table = q.first()
        table.pii_flag = pii_flag
        db.session.commit()
        return table

    def edit_column_pii_flag(self, column_id, pii_flag):
        """ update the pii flag for a column, returns column"""
        q = db.session.query(Column).filter_by(column_id=column_id)
        column = q.first()
        column.pii_flag = pii_flag
        db.session.commit()
        return column
