from google.cloud import bigquery

class BigQuerySchemasUpdater():
    self.scope = "all"
    client = bigquery.Client()

    def get_tables(project, dataset):
        # returns set of tables in the project and dataset
        dataset_id = '{0}.{1}'.format(project, dataset)
        tables = client.list_tables(dataset_id)
        return set([table.table_id for table in tables])

    def get_bq_table_schema(project, dataset, table_name):
        # returns list of bq SchemaField objects
        table_id = '{0}.{1}.{2}'.format(project, dataset, table_name)
        table = self.get_bq_table(table_id)
        return table.schema

    def get_bq_table(table_id):
        # returns bq table object for given table_id
        # the table_id format is 'project.dataset.table'
        return client.get_table(table_id)

    def get_local_table_schema(local_path, table_name):

        pass

    def update_table_schema(schema, table_name):
        pass
