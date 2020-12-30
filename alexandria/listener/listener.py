from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import Metadata
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4

from alexandria.bq_gateway import BigQueryGateway

datawarehouse_map = {
                     'bq': get_bq_gateway,
                    }

class ListenerService(object):
    def __init__(warehouse_type, db_path):
        self.refresh = 5 # compare once every 5 minutes
        self.db_path = None
        self.datawarehouse_type = 'bq'
        self.gateway = datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite://{path}/exabyte.db'.format(db_path), echo=True)

    def pull_metadata_from_ebdb(self, org_id):
        metadata_model = []
        query = select([query_table]).where(query_table.c.org_id == org_id)
        tables = self.execute_db_action('tables', query, is_select=True)
        for table_obj in tables:
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
        conn.close()
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
            add_to_ebdb(table, org_id)
        for table in change_log['remove']:
            remove_from_ebdb(table, org_id)
        for table in change_log['modify']:
            modify_ebdb(table)

    def add_to_ebdb(add_table, org_id):
        # add columns to the columns table
        add_table_id = str(uuid4())
        for column in add_table['schema']:
            add_column_id = str(uuid4())
            col_ins = cols_table.insert().values(
                          column_id = add_column_id,
                          table_id = add_table_id,
                          org_id = org_id,
                          data_type = column['field_type'],
                          name = column['name'],
                          description = column['description'],
                          annotation = '',
                          warehouse_full_column_id = '{0}.{1}'.format(add_table['full_id'], column['name']),
                          changed_time = None,
                          version = 0,
                          is_latest = True,
                      )
            self.execute_db_action('columns', col_ins, is_select=False)
        #add to the tables table
        tables_table = meta.tables['tables']
        table_ins = tables_table.insert().values(
                        table_id = add_table_id,
                        org_id = org_id,
                        name = add_table['name'],
                        description= add['description'],
                        annotation= '',
                        warehouse= self.datawarehouse_type,
                        warehouse_full_table_id = add_table['full_id'],
                        changed_time= None,
                        version= 0,
                        is_latest= True,
                    )
        self.execute_db_action('tables', table_ins, is_select=False)

    def remove_from_ebdb(remove_table):
        for column in remove_table['schema']:
            col_del = columns_table.delete().where(columns_table.c.warehouse_full_column_id == column['warehouse_full_column_id'])
            self.execute_db_action('columns', col_del, is_select=False)
        tables_table = meta.tables['tables']
        table_del = tables_table.delete().where(tables_table.c.warehouse_full_table_id == remove_table['warehouse_full_table_id'])
        self.execute_db_action('tables', table_del, is_select=False)

    def modify_ebdb(table):
        continue

    def execute_db_action(self, target_table, query, is_select=False):
        conn = self.engine.connect()
        table = meta.tables[target_table]
        res = conn.execute(query)
        if is_select:
            return [i for i in res]
            conn.close()
        conn.close()

    def get_bq_gateway():
        return BigQueryGateway()
