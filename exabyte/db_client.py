from datetime import datetime

from exabyte.models.main import (
    Org,
    TableInfo,
    ColumnInfo,
    db
)

from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData

from exabyte.alexandria.utils import get_bq_gateway


class DbClient(object):

    def get_tables_for_org(self, org_uuid):
        """ Returns all tables for an org """


        q = db.session.query(TableInfo).filter(
            TableInfo.org.has(uuid=org_uuid))

        return [
            {
                'uuid': t.uuid,
                'org_uuid': org_uuid,
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

    def table_load(self, table_uuid):
        """ returns table for a given table_id """
        q = db.session.query(TableInfo).filter_by(uuid=table_uuid)
        return q.all()

    def get_columns_for_table(self, org_uuid, table_uuid):
        """ returns all column metadata for a given table """
        # TODO (tony) enforce check that caller has access to this org

        q = db.session.query(ColumnInfo).filter(
            ColumnInfo.table_info.has(uuid=table_uuid))

        return [
            {
                'uuid': col.uuid,
                'table_uuid': table_uuid,
                'org_uuid': org_uuid,
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

    def edit_table_pii_flag(self, table_uuid, pii_flag):
        """ update the pii flag for a table, returns table"""
        q = db.session.query(TableInfo).filter_by(uuid=table_uuid)
        table = q.first()
        table.pii_flag = pii_flag
        db.session.commit()
        return table

    def edit_column_pii_flag(self, column_uuid, pii_flag):
        """ update the pii flag for a column, returns column"""
        q = db.session.query(ColumnInfo).filter_by(uuid=column_uuid)
        column = q.first()
        column.pii_flag = pii_flag
        db.session.commit()
        return column

    def update_column(self, column_uuid, **kwargs):
        # only PII is editable for now
        q = db.session.query(ColumnInfo).filter_by(uuid=column_uuid)
        column = q.first()

        if "pii_flag" in kwargs.keys():
            is_pii = bool(int(kwargs['pii_flag']))
            if is_pii != column.pii_flag:
                column.pii_flag = is_pii
                column.changed_time = datetime.now()
                column.table_info.changed_time = datetime.now()
                column.table_info.pii_column_count += 1 if is_pii else -1

        db.session.commit()

        return column.to_dict()

    def get_queries(self, query_id):
        """ get the query string and pii_flag for a given query"""
        q = db.session.query(QueryInfo).filter(QueryInfo.query_table_info.id==query_id)
        query = q.first()
        return query
