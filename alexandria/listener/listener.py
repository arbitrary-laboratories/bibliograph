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
        if dw_metadata == db_metadata:
            return None

    def enforce_changes():
        continue

    def add_to_ebdb():
        continue

    def get_bq_gateway():
        return BigQueryGateway()
