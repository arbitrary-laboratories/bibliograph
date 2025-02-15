from collections import Counter
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from exabyte.alexandria.utils import get_bq_gateway
from exabyte.alexandria.listener.listener_service import ListenerService
from exabyte.alexandria.pii_tagging.regex_pii_column_name import column_name_pii_flag
from exabyte.alexandria.pii_tagging.regex_string_pii_scanner import column_content_pii_flag

from exabyte.models.main import TableInfo

class PIIScanner(object):
    def __init__(self, db_path, org_id, sample_size=10, refresh_rate=48):
        self.org_id = org_id
        self.gateway = get_bq_gateway()
        self.db_path = db_path
        self.engine = create_engine('sqlite:///{path}'.format(path=self.db_path), echo=True)
        self.session = Session(self.engine)
        self.sample_size = sample_size
        self.listener = ListenerService(self.db_path, self.org_id)
        self.refresh_rate = refresh_rate

    def get_table_sample(self, full_table_id):
        # return a sample of data for a given table_id
        df = self.gateway.get_pandas_sample_from_table(full_table_id, self.sample_size)
        return df

    def scan_column_names(self, df):
        pii_columns = set()
        for column in df.columns:
            pii_flags = column_name_pii_flag(column)
            if len(pii_flags) > 0:
                pii_columns.add(column)
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

    def process_tables(self, tables):
        # returns the set union of the name and content scans
        for k,v in tables.items():
            column_name_pii = self.scan_column_names(v)
            column_content_pii = self.scan_column_contents(v)
            tables[k] = column_name_pii.union(column_content_pii)
        return tables

    def get_newly_added_tables(self):
        # returns a list of full table names that were newly added
        local_metadata = self.listener.pull_metadata_from_ebdb(self.org_id)
        dw_metadata = self.listener.pull_metadata_from_dw(self.org_id)
        changes = self.listener.compare_metadata(dw_metadata, local_metadata)
        return set([table['full_id'] for table in changes['add']])

    def get_stale_tables(self):
        # returns a list of full table names that were not scanned for pii recently
        tables = self.session.query(TableInfo).filter_by(org_id=self.org_id).all()
        filter_time = datetime.utcnow() - timedelta(hours=self.refresh_rate)
        stale_tables = []
        for table in tables:
            stale_table = table.TableInfoTag.filter_by(last_pii_scan <= filter_time)
            stale_tables.append(stale_table)
        return set([table['warehouse_full_table_id'] for table in stale_tables])

    def update_ebdb(self, warehouse_full_table_id, pii_set, update_time):
        # given a table_id and a set of columns detected to have pii, edit the ebdb
        update_table = self.session.query(TableInfo).filter_by(warehouse_full_table_id = warehouse_full_table_id).first()
        if update_table.pii_flag == False:
            update_table.pii_flag = True
        update_table.changed_time = update_time
        update_table.version += 1
        update_table.is_latest = True
        update_table.pii_column_count = len(pii_set)
        new_columns = []
        for update_column in update_table.column_infos:
            if update_column.name in pii_set and update_column.pii_flag == False:
                update_column.pii_flag == True
                update_column.changed_time = update_time
                update_column.version += 1
                update_column.is_latest = True
            new_columns.append(update_column)
        update_table.column_infos = new_columns
        self.session.commit()
