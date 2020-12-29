from alexandria.bq_gateway import BigQueryGateway

datawarehouse_map = {
                     'bq': get_bq_gateway,
                    }

class ListenerService(object):
    def __init__(warehouse_type, db_path):
        self.refresh = 5 # compare once every 5 minutes
        self.db_path = None
        self.gateway = datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite://{path}/exabyte.db'.format(db_path), echo=True)

    def pull_metadata_from_ebdb(self, org_id):
        metadata_model = []
        conn = self.engine.connect()
        query_table = meta.tables['tables']
        query = select([query_table]).where(query_table.c.org_id == org_id)
        tables = conn.execute(query)
        for table_obj in tables:
            query_table = meta.tables['columns']
            query = select([query_table]).where(query_table.c.table_id == table_obj['table_id'])
            columns = conn.execute(query)
            cs = []
            for col_obj in columns:
                warehouse_column_id = '{0}.{1}'.format(table_obj['warehouse_full_table_id'], col_obj['name'])
                c = {'name': col_obj['name'],
                     'description': col_obj['description'],
                     'field_type': col_obj['data_type'],
                     'warehouse_full_column_id': warehouse_column_id}
                cs.append(c)
            t = {'name': table_obj['name'],
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
                    schema, num_rows, full_id  = self.gateway.get_bq_table_metadata(project, dataset, table)
                    serialized_schema = self.gateway.serialize_schema(schema, full_id)
                    metadata_model.append({"name": table,
                                           "full_id": full_id,
                                           "schema": serialized_schema})
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
        table_changes['add'].append(added_to_add)
        modified_ids.update([item['full_id'] for item in added_to_add])

        #scenario where a table was deleted in the dw
        removed = db_tables - dw_tables
        removed_to_remove = [json.loads(table) for table in db_tables_all if json.loads(table)['full_id'] in removed]
        table_changes['remove'].append(removed_to_remove)
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


    def enforce_changes():
        continue

    def add_to_ebdb():
        continue

    def get_bq_gateway():
        return BigQueryGateway()
