import datetime
from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4

from alexandria.utils import get_bq_gateway


class ListenerService(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.gateway = get_bq_gateway() #datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite:///{path}/exabyte.db'.format(path=self.db_path), echo=True)
        self.meta = MetaData(self.engine)
        self.meta.reflect()

    def pull_metadata_from_ebdb(self, org_id):
        metadata_model = []
        query_table = self.meta.tables['tables']
        query = select([query_table]).where(query_table.c.org_id == org_id)
        tables = self.execute_db_action('tables', query, is_select=True)
        for table_obj in tables:
            query_table = self.meta.tables['columns']
            query = select([query_table]).where(query_table.c.table_id == table_obj['table_id'])
            columns = self.execute_db_action('columns', query, is_select=True)
            cs = []
            for col_obj in columns:
                warehouse_column_id = '{0}.{1}'.format(table_obj['warehouse_full_table_id'], col_obj['name'])
                c = {'name': col_obj['name'],
                     'description': col_obj['description'],
                     'field_type': col_obj['data_type'],
                     'warehouse_full_column_id': warehouse_column_id}
                cs.append(c)
            t = {'name': table_obj['name'],
                 'description': table_obj['description'],
                 'full_id': table_obj['warehouse_full_table_id'],
                 'schema': cs}
            metadata_model.append(t)
        return metadata_model

    def pull_metadata_from_dw(self, org_id):
        metadata_model = []
        for project in self.gateway.get_projects():
            for dataset in self.gateway.get_datasets(project):
                for table in self.gateway.get_tables(project, dataset):
                    description, schema, num_rows, full_id  = self.gateway.get_bq_table_metadata(project, dataset, table)
                    serialized_schema = self.gateway.serialize_schema(schema, full_id)
                    metadata_model.append({'name': table,
                                           'description': description,
                                           'full_id': full_id,
                                           'schema': serialized_schema})
        return metadata_model

    def compare_metadata(self, dw_metadata, db_metadata):
        modified_ids = set()
        table_changes = {'add':[], 'remove':[], 'modify':[]}
        db_tables_all = set([json.dumps(table) for table in db_metadata])
        dw_tables_all = set([json.dumps(table) for table in dw_metadata])

        #scenario where there are no changes to enforce
        if db_tables_all == dw_tables_all:
            return None

        # scenario where an entirely new table has been added to the dw
        db_tables = set([table['full_id'] for table in db_metadata])
        dw_tables = set([table['full_id'] for table in dw_metadata])
        added = dw_tables - db_tables
        added_to_add = [json.loads(table) for table in dw_tables_all if json.loads(table)['full_id'] in added]
        table_changes['add'] = added_to_add
        modified_ids.update([item['full_id'] for item in added_to_add])

        #scenario where a table was deleted in the dw
        removed = db_tables - dw_tables
        removed_to_remove = [json.loads(table) for table in db_tables_all if json.loads(table)['full_id'] in removed]
        table_changes['remove'] = removed_to_remove
        modified_ids.update([item['full_id'] for item in removed_to_remove])

        #scenario where a table has been edited in the dw
        for dw_table in dw_tables_all:
            dw_table = json.loads(dw_table)
            dw_table_id = dw_table['full_id']
            comp_db_table = next((json.loads(item) for item in db_tables_all if json.loads(item)['full_id'] == dw_table_id), False)
            if dw_table != comp_db_table:
                if dw_table_id not in modified_ids:
                    table_changes['modify'].append(dw_table)

        return table_changes


    def enforce_changes(self, change_log):
        for table in change_log['add']:
            self.add_to_ebdb(table, org_id)
        for table in change_log['remove']:
            self.remove_from_ebdb(table, org_id)
        for table in change_log['modify']:
            self.modify_ebdb(table)

    def add_to_ebdb(add_table, org_id):
        # add columns to the columns table
        add_table_id = str(uuid4())
        for column in add_table['schema']:
            add_column_id = str(uuid4())
            cols_table = self.meta.tables['columns']
            col_ins = cols_table.insert().values(
                          column_id = add_column_id,
                          table_id = add_table_id,
                          org_id = org_id,
                          data_type = column['field_type'],
                          name = column['name'],
                          description = column['description'],
                          pii_flag = False,
                          warehouse_full_column_id = '{0}.{1}'.format(add_table['full_id'], column['name']),
                          changed_time = None,
                          version = 0,
                          is_latest = True,
                      )
            self.execute_db_action('columns', col_ins, is_select=False)
        #add to the tables table
        tables_table = self.meta.tables['tables']
        table_ins = tables_table.insert().values(
                        table_id = add_table_id,
                        org_id = org_id,
                        name = add_table['name'],
                        description= add['description'],
                        pii_flag = False,
                        warehouse= self.datawarehouse_type,
                        warehouse_full_table_id = add_table['full_id'],
                        changed_time= None,
                        version= 0,
                        is_latest= True,
                    )
        self.execute_db_action('tables', table_ins, is_select=False)

    def remove_from_ebdb(remove_table):
        for column in remove_table['schema']:
            columns_table = self.meta.tables['columns']
            col_del = columns_table.delete().where(columns_table.c.warehouse_full_column_id == column['warehouse_full_column_id'])
            self.execute_db_action('columns', col_del, is_select=False)
        tables_table = self.meta.tables['tables']
        table_del = tables_table.delete().where(tables_table.c.warehouse_full_table_id == remove_table['warehouse_full_table_id'])
        self.execute_db_action('tables', table_del, is_select=False)

    def modify_ebdb(self, modify_table):
        tables_table = self.meta.tables['tables']
        # modify the tables table
        update_dict = {tables_table.c.name: modify_table['name'],
                       tables_table.c.description: modify_table['description'],
                       tables_table.c.changed_time: datetime.datetime.utcnow(),
                       tables_table.c.version: (tables_table.c.version + 1),
                       tables_table.c.is_latest: True,
                      }
        update_ts = tables_table.update().values(update_dict).where(tables_table.c.warehouse_full_table_id == modify_table['full_id'])
        self.execute_db_action('tables', update_ts, is_select=False)
        for column in modify_table['schema']:
            columns_table = self.meta.tables['columns']
            update_dict = {columns_table.c.name: column['name'],
                           columns_table.c.description: column['description'],
                           columns_table.c.data_type: column['field_type'],
                           columns_table.c.changed_time: datetime.datetime.utcnow(),
                           columns_table.c.version: (columns_table.c.version + 1),
                           columns_table.c.is_latest: True,
                          }
            update_cols = columns_table.update().values(update_dict).where(columns_table.c.warehouse_full_column_id == column['warehouse_full_column_id'])
            self.execute_db_action('columns', update_cols, is_select=False)


    def execute_db_action(self, target_table, query, is_select=False):
        conn = self.engine.connect()
        table = self.meta.tables[target_table]
        res = conn.execute(query)
        if is_select:
            return [i for i in res]
            conn.close()
        conn.close()
