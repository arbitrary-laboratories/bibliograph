from alexandria.data_models import (
    Org,
    TableInfo,
    ColumnInfo,
    db,
)

from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData

from alexandria.utils import get_bq_gateway


class DbClient(object):

    def get_tables_for_org(self, org_id):
        """ Returns all tables for an org """


        q = db.session.query(TableInfo).filter_by(org_id=org_id)

        return [
            {
                'uuid': t.table_id,
                'org_id': t.org_id,
                'name': t.name,
                'description': t.description,
                'pii_flag': t.pii_flag,
                'warehouse': t.warehouse,
                'warehouse_full_table_id': t.warehouse_full_table_id,
                'changed_time': t.changed_time,
                'version': t.version,
                'is_latest': t.is_latest,
            }
            for t
            in q.all()
        ]

    def table_load(self, table_id):
        """ returns table for a given table_id """
        q = db.session.query(TableInfo).filter_by(table_id=table_id)
        return q.all()

    def get_columns_for_table(self, org_id, table_id):
        """ returns all column metadata for a given table """
        # TODO (tony) enforce check that caller has access to this org

        print(table_id)
        q = db.session.query(ColumnInfo).filter_by(table_id=table_id)
        return [
            {
                'uuid': col.column_id,
                'table_id': col.table_id,
                'org_id': col.org_id,
                'data_type': col.data_type,
                'name': col.name,
                'description': col.description,
                'pii_flag': col.pii_flag,
                'warehouse_full_column_id': col.warehouse_full_column_id,
                'changed_time': col.changed_time,
                'version': col.version,
                'is_latest': col.is_latest,
            }
            for col
            in q.all()
        ]

    def edit_table_pii_flag(self, table_id, pii_flag):
        """ update the pii flag for a table, returns table"""
        q = db.session.query(TableInfo).filter_by(table_id=table_id)
        table = q.first()
        table.pii_flag = pii_flag
        db.session.commit()
        return table

    def edit_column_pii_flag(self, column_id, pii_flag):
        """ update the pii flag for a column, returns column"""
        q = db.session.query(ColumnInfo).filter_by(column_id=column_id)
        column = q.first()
        column.pii_flag = pii_flag
        db.session.commit()
        return column
