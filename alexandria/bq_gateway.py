from google.cloud import bigquery

class BigQueryGateway():
    client = bigquery.Client()

    def get_tables(project, dataset):
        # returns set of tables in the project and dataset
        dataset_id = '{0}.{1}'.format(project, dataset)
        tables = client.list_tables(dataset_id)
        return set([table.table_id for table in tables])

    def get_bq_table_metadata(project, dataset, table_name):
        # returns list of bq SchemaField objects
        table_id = '{0}.{1}.{2}'.format(project, dataset, table_name)
        table = client.get_table(table_id)
        return table.schema, table.num_rows

    def create_schema_object_from_json(schema_struct):
        # returns a valid bigquery schema from locally storeed json representation
        schema = []
        for i in schema_struct:
            schema.append(bigquery.schema.SchemaField.from_api_repr(i))
        return schema

    def update_table_schema(full_table_location, table_description, schema_struct, table_name):
        # full_table_location: str, project.dataset.table
        # table_description: str, table description to add
        # schema_struct: list of dicts, representing a valid schema that matches the
        # schema of full_table_location with descriptions to add to each column
        client = bigquery.Client()
        table = client.get_table(full_table_location)
        table.description = table_description

        schema = create_schema_object_from_json(schema_struct)
        table.schema = schema
        client.update_table(table, ['description', 'schema'])

    def get_query_history():
        client = bigquery.Client()
        jobs = client.list_jobs(all_users=True)
        return [job.query for job in jobs]
