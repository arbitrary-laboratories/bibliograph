import datetime
from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from uuid import uuid4

from alexandria.utils import get_bq_gateway, extract_tables

from alexandria.data_models import (
    Org,
    TableInfo,
    ColumnInfo,
    db,
)


class ListenerService(object):
    def __init__(self, db_path, org_id):
        self.interval = 5 # how often, in minutes to poll the dw
        self.db_path = db_path
        self.gateway = get_bq_gateway() #datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite:///{path}/exabyte.db'.format(path=self.db_path), echo=True)
        self.meta = MetaData(self.engine)
        self.meta.reflect()
        self.max_table_id = db.session.query(func.max(TableInfo.id).label("max_id")).one().max_id
        self.max_column_id = db.session.query(func.max(TableInfo.id).label("max_id")).one().max_id

    def pull_metadata_from_ebdb(self, org_id):
        metadata_model = []
        tables = db.session.query(TableInfo).filter(TableInfo.org_id == org_id)
        for table_obj in tables.all():
            columns = table_obj.column_infos
            cs = []
            for col_obj in columns:
                warehouse_column_id = '{0}.{1}'.format(table_obj.warehouse_full_table_id, col_obj.name)
                c = {'name': col_obj.name,
                     'description': col_obj.description,
                     'field_type': col_obj.data_type,
                     'warehouse_full_column_id': warehouse_column_id}
                cs.append(c)
            t = {'name': table_obj.name,
                 'description': table_obj.description,
                 'full_id': table_obj.warehouse_full_table_id,
                 'schema': cs}
            metadata_model.append(t)
        return metadata_model

    def pull_metadata_from_dw(self, org_id):
        metadata_model = []
        for project in self.gateway.get_projects():
            for dataset in self.gateway.get_datasets(project):
                for table in self.gateway.get_tables(project, dataset):
                    description, schema, full_id  = self.gateway.get_bq_table_metadata(project, dataset, table)
                    serialized_schema = self.gateway.serialize_schema(schema, full_id)
                    metadata_model.append({'name': table,
                                           'description': description,
                                           'full_id': full_id,
                                           'schema': serialized_schema})
        return metadata_model

    def compare_metadata(self, dw_metadata, db_metadata):
        modified_ids = set()
        table_changes = {'add':[], 'remove':[], 'modify':[]}
        db_tables_all = set([json.dumps(table) for table in db_metadata])
        dw_tables_all = set([json.dumps(table) for table in dw_metadata])

        #scenario where there are no changes to enforce
        if db_tables_all == dw_tables_all:
            return None

        # scenario where an entirely new table has been added to the dw
        db_tables = set([table['full_id'] for table in db_metadata])
        dw_tables = set([table['full_id'] for table in dw_metadata])
        added = dw_tables - db_tables
        added_to_add = [json.loads(table) for table in dw_tables_all if json.loads(table)['full_id'] in added]
        table_changes['add'] = added_to_add
        modified_ids.update([item['full_id'] for item in added_to_add])

        #scenario where a table was deleted in the dw
        removed = db_tables - dw_tables
        removed_to_remove = [json.loads(table) for table in db_tables_all if json.loads(table)['full_id'] in removed]
        table_changes['remove'] = removed_to_remove
        modified_ids.update([item['full_id'] for item in removed_to_remove])

        #scenario where a table has been edited in the dw
        for dw_table in dw_tables_all:
            dw_table = json.loads(dw_table)
            dw_table_id = dw_table['full_id']
            comp_db_table = next((json.loads(item) for item in db_tables_all if json.loads(item)['full_id'] == dw_table_id), False)
            if dw_table != comp_db_table:
                if dw_table_id not in modified_ids:
                    table_changes['modify'].append(dw_table)

        return table_changes


    def enforce_changes(self, change_log):
        for table in change_log['add']:
            self.add_to_ebdb(table, org_id)
        for table in change_log['remove']:
            self.remove_from_ebdb(table, org_id)
        for table in change_log['modify']:
            self.modify_ebdb(table)

    def add_to_ebdb(add_table, org_id):
        add_table_id = str(uuid4())
        add_columns = []
        for column in add_table['schema']:
            add_column_id = str(uuid4())
            insert_column = ColumnInfo(
                              id = (self.max_column_id + 1),
                              uuid = add_column_id,
                              table_info_id = (self.max_table_id + 1),
                              org_id = org_id,
                              data_type = column['field_type'],
                              name = column['name'],
                              description = column['description'],
                              pii_flag = False,
                              warehouse_full_column_id = '{0}.{1}'.format(add_table['full_id'], column['name']),
                              changed_time = None,
                              version = 0,
                              is_latest = True,
                            )
            add_columns.append(insert_column)
            self.max_column_id += 1
        insert_table = TableInfo(
                        table_id = add_table_id,
                        org_id = org_id,
                        name = add_table['name'],
                        description= add['description'],
                        pii_flag = False,
                        warehouse= self.datawarehouse_type,
                        warehouse_full_table_id = add_table['full_id'],
                        changed_time= None,
                        version= 0,
                        is_latest= True,
                        column_infos= add_columns,
                    )
        self.max_table_id += 1
        db.session.add(insert_table)
        db.session.commit()

    def remove_from_ebdb(remove_table):
        table_del = db.session.query(TableInfo).filter(TableInfo.warehouse_full_table_id == remove_table['warehouse_full_table_id'])
        table_del.column_infos.delete()
        table_del.delete()
        db.session.commit()

    def modify_ebdb(self, modify_table):
        update_table = db.session.query(TableInfo).filter_by(warehouse_full_table_id = modify_table['full_id'])
        update_table.name = modify_table['name']
        update_table.description = modify_table['description']
        update_table.changed_time = datetime.datetime.utcnow()
        update_table.version = (tables_table.c.version + 1)
        update_table.is_latest = True
        update_columns = []
        for column in modify_table['schema']:
            update_column = update_table.column_infos.filter_by(warehouse_full_column_id = column['warehouse_full_column_id'])
            update_column.name = column['name'],
            update_column.description = column['description']
            update_column.data_type = column['field_type']
            update_column.changed_time = datetime.datetime.utcnow()
            update_column.version = (columns_table.c.version + 1)
            update_column.is_latest = True
            update_columns.append(update_column)
        update_table.column_infos = update_columns
        db.session.commit()

    def update_queries(self, min_creation_time, max_creation_time):
        jobs = self.gateway.paginated_call_list_jobs(min_creation_time, max_creation_time)
        queries = [i[1] for i in jobs if i[0] == 'query']
        for query in queries:
            mentioned_tables = extract_tables(query)
            query_uuid = str(uuid4())
            insert_query_info = QueryInfo(
                                    uuid = query_uuid,
                                    query_string = query,
            )
            query_table_infos = []
            for table in mentioned_tables:
                query_table_uuid = str(uuid4())
                try:
                    extracted_table_name = get_extracted_table_name(table)
                    table_obj = db.session.query(TableInfo).filter(TableInfo.warehouse_full_table_id == extracted_table_name)
                    insert_query_table_info = QueryTableInfo(
                                                  uuid = query_table_uuid,
                                                  table_info = table_obj,
                                                  pii_flag = table_obj.pii_flag,
                                                  )

                    query_table_infos.append(insert_query_table_info)
                except:
                    # not correct format TODO -- make this broader
                    continue
            insert_query_info.query_table_info = query_table_infos
            db.session.commit()


    def get_extracted_table_name(self, full_table_id):
        full_table_id = full_table_id.replace('`', '')
        project, dataset, table = full_table_id.split('.')
        return '{0}:{1}.{2}'.format(project, dataset, table)


    def execute_db_action(self, target_table, query, is_select=False):
        conn = self.engine.connect()
        table = self.meta.tables[target_table]
        res = conn.execute(query)
        if is_select:
            return [i for i in res]
            conn.close()
        conn.close()
