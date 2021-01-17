import time
import schedule
from datetime import datetime, timedelta
from exabyte.alexandria.listener.listener_service import ListenerService

def run(service,  org_id, start_time=None, end_time=None, queries=False):
    # interval is a timedelta
    # time_last_executed is a datetime
    db_metadata = service.pull_metadata_from_ebdb(org_id)
    dw_metadata = service.pull_metadata_from_dw(org_id)
    changes = service.compare_metadata(dw_metadata, db_metadata)
    service.enforce_changes(changes, org_id)
    if queries:
        num_queries = service.update_queries(start_time,
                                             end_time)
        print('{} queries added'.format(num_queries))

def update_job():
    start_time = datetime.utcnow() - timedelta(minutes=1)
    end_time = datetime.utcnow()
    run(listener,
        org_id = 1,
        start_time= start_time,
        end_time= end_time,
        queries=True)

if __name__ == "__main__":
    listener = ListenerService(db_path="/Users/girish/bibliograph/exabyte_test.db",
                                org_id=1)
    run(listener, org_id=1)
    init_queries = listener.update_queries(datetime(2020, 12, 1), datetime.now())
    schedule.every(1).minutes.do(update_job)
    while True:
        schedule.run_pending()
        time.sleep(1)
