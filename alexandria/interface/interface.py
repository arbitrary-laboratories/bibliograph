from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import Metadata
from sqlalchemy.ext.declarative import declarative_base

from alexandria.bq_gateway import BigQueryGateway

datawarehouse_map = {
                     'bq': get_bq_gateway,
                    }

class InterfaceService(object):
    def __init__(warehouse_type, db_path):
        self.db_path = None
        self.datawarehouse_type = 'bq'
        self.gateway = datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite://{path}/exabyte.db'.format(db_path), echo=True)
        self.meta = MetaData(self.engine)
        self.meta.reflect()

    def org_init(org_id, init=True):
        # sends all table for an organiztation
        tables_table = self.meta.tables['tables']
        tables_sel = select([tables_table]).where(tables_table.c.org_id == org_id)
        results = execute_db_action('tables', tables_sel, is_select=True)
        return [(i.table_id,
                 i.name,
                 i.description,
                 i.annotation,
                 i.warehouse_full_table_id) for i in results]

    def table_load(table_id):
        # sends all column data for a given table
        tables_table = self.meta.tables['tables']
        tables_sel = select([tables_table]).where(tables_table.c.table_id == table_id)
        results = execute_db_action('tables', tables_sel, is_select=True)
        return results

    def columns_load(table_id):
        # sends all column data for a given table
        columns_table = self.meta.tables['columns']
        columns_sel = select([columns_table]).where(columns_table.c.table_id == table_id)
        results = execute_db_action('columns', columns_sel, is_select=True)
        return results

    def edit_annotation(id, edit_target, changed_annotation):
        # id is either column_id or table_id
        # edit_target is either 'columns' or 'tables'
        table = self.meta.tables[edit_target]
        update_dict = {table.c.annotation: changed_annotation}
        if edit_target == 'columns':
            update = table.update().values(update_dict).where(table.c.column_id == id)
        elif edit_target == 'tables':
            update = table.update().values(update_dict).where(table.c.table_id == id)
        self.execute_db_action(edit_target, update, is_select=False)


    def execute_db_action(self, target_table, query, is_select=False):
        conn = self.engine.connect()
        table = self.meta.tables[target_table]
        res = conn.execute(query)
        if is_select:
            return [i for i in res]
            conn.close()
        conn.close()

    def get_bq_gateway():
        return BigQueryGateway()
