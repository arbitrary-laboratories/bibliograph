from google.cloud import bigquery
import os
import pickle as pkl
import json

class BigQueryGateway(object):
    def __init__(self):
        self.client = bigquery.Client()#.from_service_account_json("path/to/key.json")

    def get_projects(self):
        # returns list of projects associated with the service account scope
        return [project.friendly_name for project in self.client.list_projects()]

    def get_datasets(self, project):
        return [dataset.dataset_id for dataset in self.client.list_datasets(project)]

    def get_tables(self, project, dataset):
        # returns set of tables in the project and dataset
        dataset_id = '{0}.{1}'.format(project, dataset)
        tables = self.client.list_tables(dataset_id)
        return [table.table_id for table in tables]

    def get_bq_table_metadata(self, project, dataset, table_name):
        # returns list of bq SchemaField objects
        table_id = '{0}.{1}.{2}'.format(project, dataset, table_name)
        table = self.client.get_table(table_id)
        return table.description, table.schema, table.full_table_id

    def get_pandas_sample_from_table(self, project, dataset, table_name, sample_size):
        # sample_size is the count of samples needed
        query = """SELECT * FROM `{p}.{d}.{t}` WHERE {s}/(SELECT COUNT(*) FROM `{p}.{d}.{t}`)
        """.format(p=project, d=dataset, t=table_name, s=sample_size)
        return pd.read_gbq(query)

    def create_schema_object_from_json(self, schema_struct):
        # returns a valid bigquery schema from locally stored json representation
        schema = []
        for i in schema_struct:
            schema.append(bigquery.schema.SchemaField.from_api_repr(i))
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

        schema = self.create_schema_object_from_json(schema_struct)
        table.schema = schema
        self.client.update_table(table, ['description', 'schema'])

    def get_last_jobs(self,
                        min_creation_time,
                        page_token=None):
        jobs = self.pagination_call_list_jobs(min_creation_time, results_returned)
        return [self.process_job(i) for i in jobs]

    def paginated_call_list_jobs(self,
                                 min_creation_time,
                                 max_creation_time):
        current_page = self.client.list_jobs(all_users=True,
                                             min_creation_time=min_creation_time,
                                             max_creation_time=max_creation_time)
        accumulated_jobs = self.get_page_jobs(current_page)
        while current_page.next_page_token is not None:
            next_page = self.client.list_jobs(all_users=True,
                                              page_token = current_page.next_page_token,
                                              min_creation_time=min_creation_time,
                                              max_creation_time=max_creation_time)
            next_page_resources = self.get_page_jobs(next_page)
            accumulated_jobs.extend(next_page_resources)
            current_page = next_page
        return [self.process_job(job) for job  in accumulated_jobs]

    def get_page_jobs(self, page):
        return list(page)

    def process_job(self, job_obj):
        if job_obj.job_type == 'load':
            return (job_obj.job_type, job_obj.destination)
        elif job_obj.job_type == 'query':
            return (job_obj.job_type, job_obj.query, job_obj.user_email, job_obj.created)
        elif job_obj.job_type == 'copy':
            return (job_obj.job_type, job_obj.destination)
        elif job_obj.job_type == 'extract':
            return (job_obj.job_type, job_obj.destination_uris)

    def serialize_schema(self, schema_obj, full_table_id):
        ret_list = []
        for val in schema_obj:
            ret_list.append({
                'name': val.name,
                'description': val.description,
                'field_type': val.field_type,
                'warehouse_full_column_id': '{0}.{1}'.format(full_table_id, val.name),
            })
        return ret_list
