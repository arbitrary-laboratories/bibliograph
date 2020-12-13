from google.cloud import bigquery

class BigQueryGateway():
    def __init__(self):
        self.client = bigquery.Client()

    def get_tables(self, project, dataset):
        # returns set of tables in the project and dataset
        dataset_id = '{0}.{1}'.format(project, dataset)
        tables = self.client.list_tables(dataset_id)
        return [table.table_id for table in tables]

    def get_bq_table_metadata(self, project, dataset, table_name):
        # returns list of bq SchemaField objects
        table_id = '{0}.{1}.{2}'.format(project, dataset, table_name)
        table = self.client.get_table(table_id)
        return table.schema, table.num_rows

    def create_schema_object_from_json(self, schema_struct):
        # returns a valid bigquery schema from locally storeed json representation
        schema = []
        for i in schema_struct:
            schema.append(bigquery.schema.SchemaField.from_api_repr(i))
        return schema

    def update_table_schema(self, full_table_location, table_description, schema_struct, table_name):
        # full_table_location: str, project.dataset.table
        # table_description: str, table description to add
        # schema_struct: list of dicts, representing a valid schema that matches the
        # schema of full_table_location with descriptions to add to each column
        table = self.client.get_table(full_table_location)
        table.description = table_description

        schema = create_schema_object_from_json(schema_struct)
        table.schema = schema
        self.client.update_table(table, ['description', 'schema'])

    def get_query_history(self):
        jobs = self.client.list_jobs(all_users=True)
        return [job.query for job in jobs]
