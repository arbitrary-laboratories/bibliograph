from google.cloud import bigquery
from alexandria.schema_obj import SchemaObject
import os
import pickle as pkl

class BigQueryGateway(object):
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
        # returns a valid bigquery schema from locally stored json representation
        schema = []
        for i in schema_struct:
            schema.append(bigquery.schema.SchemaField.from_api_repr(i))
        return schema

    def create_json_from_schema_object(self, schema_obj):
        # returns valid locally stored json representation from a bigquery schema
        schema = []
        for i in schema_obj:
            schema.append(bigquery.schema.SchemaField.to_api_repr(i))
        return schema

    def update_warehouse_table_schema(self,
                                      project,
                                      dataset,
                                      table_name,
                                      table_description,
                                      schema_struct):
        # full_table_location: str, project.dataset.table
        # table_description: str, table description to add
        # schema_struct: list of dicts, representing a valid schema that matches
        # the schema of full_table_location with descriptions to add to each
        # columns
        full_table_location = '{0}.{1}.{2}'.format(project, dataset, table_name)
        table = self.client.get_table(full_table_location)
        table.description = table_description

        schema = create_schema_object_from_json(schema_struct)
        table.schema = schema
        self.client.update_table(table, ['description', 'schema'])

    def update_local_table_schema(self,
                                  save_path,
                                  project,
                                  dataset,
                                  table_name,
                                  table_description,
                                  schema_struct):
        # full_table_location: str, project.dataset.table
        # table_description: str, table description to add
        # schema_struct: list of dicts, representing a valid schema that matches
        # the schema of full_table_location with descriptions to add to each
        # column
        save_path += '{0}_{1}_{2}_schema.pkl'.format(project,
                                                     dataset,
                                                     table_name)
        save_schema = SchemaObject(table_description,
                                   create_json_from_schema_object(schema_struct))
        with open(save_path, 'wb') as f:
            pkl.dump(save_schema, f)

    def get_query_history(self):
        jobs = self.client.list_jobs(all_users=True)
        return [job.query for job in jobs]

    def serialize_schema(self, schema_obj):
        ret_list = []
        for val in schema_obj:
            ret_list.append({
                'name': val.name,
                'description': val.description,
                'field_type': val.field_type,
                'fields': val.fields,
                'is_nullable': val.is_nullable,
                'mode': val.mode,
                'policy_tags': val.policy_tags,
            })
        return ret_list
