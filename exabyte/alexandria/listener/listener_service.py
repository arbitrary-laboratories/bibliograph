from datetime import datetime
import json
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy import insert, select, delete, inspect
from sqlalchemy import Column, DateTime, Integer, String, Boolean
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func

from exabyte.alexandria.utils import get_bq_gateway, extract_tables
from exabyte.models.main import (
    metadata,
    Org,
    TableInfo,
    ColumnInfo,
    QueryInfo,
    QueryTableInfo,
)


class ListenerService(object):
    def __init__(self, db_path, org_id):
        self.db_path = db_path
        self.gateway = get_bq_gateway() #datawarehouse_map[warehouse_type]()
        self.engine = create_engine('sqlite:///{path}'.format(path=self.db_path), echo=True)
        self.session = Session(self.engine)

    def pull_metadata_from_ebdb(self, org_id):
        metadata_model = []
        tables = self.session.query(TableInfo).filter(TableInfo.org_id == org_id)
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
            return table_changes

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


    def enforce_changes(self, change_log, org_id):
        # change_log is the result of the compare metadata function
        for table in change_log['add']:
            self.add_to_ebdb(table, org_id)
        for table in change_log['remove']:
            self.remove_from_ebdb(table)
        for table in change_log['modify']:
            self.modify_ebdb(table)

    def add_to_ebdb(self, add_table, org_id):
        org = self.session.query(Org).filter_by(id=org_id).first()
        add_columns = []
        insert_table = TableInfo(
                    org = org,
                    name = add_table['name'],
                    description= add_table['description'],
                    annotation = None,
                    pii_flag = False,
                    warehouse= 'bq',
                    warehouse_full_table_id = add_table['full_id'],
                    version= 0,
                    is_latest= True,
                )
        for column in add_table['schema']:
            insert_column = ColumnInfo(
                              table_info = insert_table,
                              data_type = column['field_type'],
                              name = column['name'],
                              description = column['description'],
                              annotation = None,
                              pii_flag = False,
                              warehouse_full_column_id = '{0}.{1}'.format(add_table['full_id'], column['name']),
                              version = 0,
                              is_latest = True,
                            )
            add_columns.append(insert_column)
        insert_table.column_infos = add_columns
        self.session.add(insert_table)
        self.session.commit()

    def remove_from_ebdb(self, remove_table):
        table_del = self.session.query(TableInfo).filter(TableInfo.warehouse_full_table_id == remove_table['warehouse_full_table_id'])
        tables = table_del.all()
        for table in tables:
            for column in table.column_infos:
                # delete each column object
                self.session.delete(column)
        # delete this query object
        table_del.delete()
        self.session.commit()

    def modify_ebdb(self, modify_table):
        update_table = self.session.query(TableInfo).filter_by(warehouse_full_table_id = modify_table['full_id'])
        update_table.name = modify_table['name']
        update_table.description = modify_table['description']
        update_table.changed_time = datetime.now()
        update_table.version = (update_table.first().version + 1)
        update_table.is_latest = True
        update_columns = []
        for column in modify_table['schema']:
            update_column = self.session.query(ColumnInfo).filter_by(warehouse_full_column_id = column['warehouse_full_column_id'])
            update_column.name = column['name'],
            update_column.description = column['description']
            update_column.data_type = column['field_type']
            update_column.changed_time = datetime.now()
            update_column.version = (update_column.first().version + 1)
            update_column.is_latest = True
            update_columns.append(update_column)
        update_table.column_infos = update_columns
        self.session.commit()

    def update_queries(self, min_creation_time, max_creation_time):
        jobs = self.gateway.paginated_call_list_jobs(min_creation_time, max_creation_time)
        queries = [{'query_string':i[1],'account':i[2]} for i in jobs if i[0] == 'query']
        # (query_string, user_email)
        for query in queries:
            mentioned_tables = extract_tables(query['query_string'])
            insert_query_info = QueryInfo(
                                    query_string = query['query_string'],
                                    account = query['account'],
                                    )
            query_table_infos = []
            for table in mentioned_tables:
                try:
                    extracted_table_name = self.get_extracted_table_name(table)
                    table_obj = self.session.query(TableInfo).filter(TableInfo.warehouse_full_table_id == extracted_table_name).first()
                    insert_query_table_info = QueryTableInfo(
                                                  table_info = table_obj,
                                                  query_info = insert_query_info,
                                                  pii_flag = table_obj.pii_flag,
                                                  )

                    query_table_infos.append(insert_query_table_info)
                except:
                    # not correct format TODO -- make this broader
                    continue
            insert_query_info.query_table_info = query_table_infos
            self.session.add(insert_query_info)
            self.session.commit()
        return len(queries)


    def get_extracted_table_name(self, full_table_id):
        full_table_id = full_table_id.replace('`', '')
        project, dataset, table = full_table_id.split('.')
        return '{0}:{1}.{2}'.format(project, dataset, table)
