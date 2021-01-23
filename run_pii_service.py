import argparse
import time
import schedule
from datetime import datetime, timedelta

from exabyte.alexandria.utils import get_bq_gateway
from exabyte.alexandria.pii_tagging.pii_service import PIIScanner

def run(service, org_id, init=False):
    # get all required tables
    now_time = datetime.utcnow()
    if init==True:
        tables_to_scan = get_all_tables(org_id)
    else:
        new_tables = service.get_newly_added_tables()
        stale_tables = service.get_stale_tables()
        tables_to_scan = new_tables.union(stale_tables)
    tables = {}
    for table in tables_to_scan:
        tables[table] = service.get_table_sample(table)
    processed_tables = service.process_tables(tables)
    for table, pii_cols in processed_tables.items():
        service.update_ebdb(table, pii_cols, now_time)

def get_all_tables(org_id):
    tables = []
    gateway = get_bq_gateway()
    for project in gateway.get_projects():
        for dataset in gateway.get_datasets(project):
            for table in gateway.get_tables(project, dataset):
                tables.append(table['full_id'])
    return tables

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('org_id')
    parser.add_argument('db_path')
    parser.add_argument('--init', action='store_true', default=False)
    args = parser.parse_args()

    pii_service = PIIScanner(args.db_path, args.org_id, args.init)
    run(pii_service, args.org_id)
    # if init, get all tables
