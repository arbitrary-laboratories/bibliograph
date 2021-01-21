import argparse
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

def update_job(org_id, run_interval):
    start_time = datetime.utcnow() - timedelta(minutes=run_interval)
    end_time = datetime.utcnow()
    run(listener,
        org_id = org_id,
        start_time= start_time,
        end_time= end_time,
        queries=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('org_id')
    parser.add_argument('--run_interval', default=1)
    parser.add_argument('--init', action='store_true', default=False)
    args = parser.parse_args()

    listener = ListenerService(db_path="path here",
                                org_id=args.org_id)
    if args.init:
        run(listener, org_id=args.org_id)
        init_queries = listener.update_queries(datetime(2020, 12, 1), datetime.now())
    schedule.every(args.run_interval).minutes.do(lambda: update_job(args.org_id, args.run_interval))
    while True:
        schedule.run_pending()
        time.sleep(1)
