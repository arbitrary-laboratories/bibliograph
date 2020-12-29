from alexandria.bq_gateway import BigQueryGateway

datawarehouse_map = {
                     'bq': get_bq_gateway,
                    }

class ListenerService(object):
    def __init__(warehouse_type):
        self.refresh = 5 # compare once every 5 minutes
        self.db_path = None
        self.gateway = datawarehouse_map[warehouse_type]()

    def pull_metadata_from_ebdb(org_id):
        metadata_model = []
        conn = engine.connect()
        query_table = meta.tables['tables']
        query = select([query_table]).where(query_table.c.org_id == '549fc93b-3c16-48fc-b37d-dee697623bd0')
        tables = conn.execute(query)
        for table_obj in tables:
            query_table = meta.tables['columns']
            query = select([query_table]).where(query_table.c.table_id == table_obj['table_id'])
            columns = conn.execute(query)
            cs = []
            for col_obj in columns:
                c = {'name': col_obj['name'], 'description':col_obj['description'], 'field_type':col_obj['data_type']}
                cs.append(c)
            t = {'name': table_obj['name'], "schema": cs}
            metadata_model.append(t)
        conn.close()
        return metadata_model

    def pull_metadata_from_dw(org_id):
        metadata_model = []
        for project in projects:
            for dataset in g.get_datasets(project):
                for table in g.get_tables(project, dataset):
                    schema, num_rows = g.get_bq_table_metadata(project, dataset, table)
                    serialized_schema = g.serialize_schema(schema)
                    metadata_model.append({"name": table, "schema": serialized_schema})
        return metadata_model

    def compare_metadata(dw_metadata, db_metadata):
        continue

    def enforce_changes():
        continue

    def add_to_ebdb():
        continue

    def get_bq_gateway():
        return BigQueryGateway()
