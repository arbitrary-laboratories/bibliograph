from collections import Counter
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from exabyte.alexandria.utils import get_bq_gateway
from exabyte.alexandria.pii_tagging.regex_pii_column_name import column_name_pii_flag
from exabyte.alexandria.pii_tagging.regex_string_pii_scanner import column_content_pii_flag

class PIIScanner(object):
    def __init__(self, db_path, org_id, sample_size=10):
        self.org_id = org_id
        self.gateway = get_bq_gateway()
        self.db_path = db_path
        self.engine = create_engine('sqlite:///{path}'.format(path=self.db_path), echo=True)
        self.session = Session(self.engine)
        self.sample_size = sample_size

    def get_tables(self, org_id):
        samples = {}
        for project in self.gateway.get_projects():
            for dataset in self.gateway.get_datasets(project):
                for table in self.gateway.get_tables(project, dataset):
                    warehouse_full_table_id = "{0}:{1}.{2}".format(project, dataset, table)
                    df = self.gateway.get_pandas_sample_from_table(project, dataset, table, self.sample_size)
                    samples[warehouse_full_table_id] = df
        return samples

    def scan_column_names(self, df):
        pii_columns = set()
        for column in df.columns:
            pii_flags = column_name_pii_flag(column)
            if len(pii_flags) > 0:
                pii_column.add(column)
        return pii_columns

    def scan_column_contents(self, df):
        pii_columns = set()
        for column in df.columns:
            values = df[column].values
            counter = Counter()
            for value in values:
                pii_flags = column_content_pii_flag(str(value))
                for flag in pii_flags:
                    counter.update({flag: 1})
            most_common = counter.most_common()
            if len(most_common)>0 and most_common[0][1] >5:
                pii_columns.add(column)
        return pii_columns

    def process_tables(self, org_id):
        # is the set union of the name and content scans
        tables = self.get_tables(org_id)
        for k,v in tables.items():
            column_name_pii = self.scan_column_names(v)
            column_content_pii = self.scan_column_contents(v)
            tables[k] = column_name_pii.union(column_content_pii)
        return tables

    # def get_new_data(self, org_id):
    #     # returns a list of full table names that were newly added
    #     continue
    #
    # def get_stale_data(self, org_id):
    #     # returns a list of full table names that were not scanned for pii recently
    #     self.session.query(Table)
    #
    #
    # def update_ebdb(self, table_name, table_pii):
    #     table = self.session.query(TableInfo).filter_by(warehouse_full_table_id=table_name).first()
