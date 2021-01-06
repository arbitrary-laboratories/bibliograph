import settings

from flask import Flask, request, jsonify
from flask_cors import CORS
from alexandria.data_models import db
from db_client import DbClient

from alexandria.bq_gateway import BigQueryGateway

app = Flask(__name__)
app.config.from_object(settings)
db.init_app(app)

# Temporary
CORS(app)


@app.route('/orgs/<org_uuid>/tables', methods=['GET'])
def get_tables_for_org(org_uuid):
    client = DbClient()
    res = client.get_tables_for_org(org_uuid)
    return jsonify(res)

@app.route('/orgs/<org_uuid>/tables/<table_uuid>/columns', methods=['GET'])
def get_columns_for_table(org_uuid, table_uuid):
    client = DbClient()
    res = client.get_columns_for_table(org_uuid, table_uuid)
    return jsonify(res)

# TODO Deprecate
@app.route('/tables', methods=['GET'])
def get_project_tables():
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    gateway = BigQueryGateway()
    tables = gateway.get_tables(project, dataset)
    return jsonify(tables)

# TODO Deprecate
@app.route('/table_metadata', methods=['GET'])
def get_metadata():
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    table_name = request.args.get('tablename')

    gateway = BigQueryGateway()
    description, schema, num_rows, table_id = gateway.get_bq_table_metadata(project, dataset, table_name)
    return jsonify([gateway.serialize_schema(schema, table_id), description, num_rows])

@app.route('/update_local_schema', methods=['POST'])
def update_local_schema():
    # update the local file that contains the table metadata
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    table_name = request.args.get('tablename')
    data = request.get_json()
    gateway = BigQueryGateway()
    # TODO: Where to store these schema representations?
    gateway.update_local_table_schema('',
                                      project,
                                      dataset,
                                      table_name,
                                      data['description'],
                                      data['schema_struct'])

@app.route('/update_wh_from_local', methods=['POST'])
def update_warehouse_schema():
    # TODO: Where to store these schema representations?
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    table_name = request.args.get('tablename')
    data = request.get_json()
    gateway =  BigQueryGateway()
    gateway.update_warehouse_table_schema(project,
                                          dataset,
                                          table_name,
                                          data['description'],
                                          data['schema_struct'])

@app.route('/query_history', methods=['GET'])
def get_qh():
    gateway = BigQueryGateway()
    return jsonify(gateway.get_query_history())

if __name__ == '__main__':
    app.run()

    with app.app_context():
        db.create_all()
