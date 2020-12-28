from alexandria.bq_gateway import BigQueryGateway

datawarehouse_map = {
                     'bq': get_bq_gateway,
                    }

class ListenerService(object):
    def __init__(warehouse_type):
        self.listener = None
        self.refresh = 5 # compare once every 5 minutes
        self.db_path = None
        self.gateway = datawarehouse_map[warehouse_type]()

    def pull_metadata_from_ebdb(org_id):
        continue

    def pull_metadata_from_dw(org_id):
        metadata_model = {"projects":[]}
        for project in projects:
            ds = []
            for dataset in g.get_datasets(project):
                ts = []
                for table in g.get_tables(project, dataset):
                    schema, num_rows = g.get_bq_table_metadata(project, dataset, table)
                    serialized_schema = g.serialize_schema(schema)
                    ts.append({"name": table, "schema": serialized_schema, "num_rows": num_rows})
                ds.append({'name': dataset, "tables": ts})
            metadata_model['projects'].append({"name": project, "datasets":ds})
        return metadata_model

    def compare_metadata():
        continue

    def enforce_changes():
        continue

    def get_bq_gateway():
        return BigQueryGateway()
