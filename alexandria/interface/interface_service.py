from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

from alexandria.utils import get_bq_gateway

# datawarehouse_map = {
#                      'bq': get_bq_gateway,
#                     }

class InterfaceService(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.gateway = get_bq_gateway() #datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite:///{path}/exabyte.db'.format(path=self.db_path), echo=True)
        self.meta = MetaData(self.engine)
        self.meta.reflect()

    def org_init(self, org_id, init=True):
        # sends all table for an organiztation
        tables_table = self.meta.tables['tables']
        tables_sel = select([tables_table]).where(tables_table.c.org_id == org_id)
        results = self.execute_db_action('tables', tables_sel, is_select=True)
        return [(i.table_id,
                 i.name,
                 i.description,
                 i.pii_flag,
                 i.warehouse_full_table_id) for i in results]

    def table_load(self, table_id):
        # sends all column data for a given table
        tables_table = self.meta.tables['tables']
        tables_sel = select([tables_table]).where(tables_table.c.table_id == table_id)
        results = self.execute_db_action('tables', tables_sel, is_select=True)
        return results

    def columns_load(self, table_id):
        # sends all column data for a given table
        columns_table = self.meta.tables['columns']
        columns_sel = select([columns_table]).where(columns_table.c.table_id == table_id)
        results = self.execute_db_action('columns', columns_sel, is_select=True)
        return results

    def edit_ii_flag(self, id, edit_target, pii_flag):
        # id is either column_id or table_id
        # edit_target is either 'columns' or 'tables'
        # pii_flag is a boolean
        table = self.meta.tables[edit_target]
        update_dict = {table.c.pii_flag: pii_flag}
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
