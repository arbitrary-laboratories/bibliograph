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


@app.route('/orgs/<org_uuid>/tables/<table_uuid>/columns/<column_uuid>', methods=['GET', 'PATCH'])
def update_column_info(org_uuid, table_uuid, column_uuid):
    client = DbClient()
    res = None
    if request.method == 'GET':
        return "" # TODO

    if request.method == 'PATCH':
        form = request.form
        res = client.update_column(column_uuid, **form)

    return jsonify(res)


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

# @app.route('/job_history', methods=['GET'])
# def get_jh():
#     gateway = BigQueryGateway()
#     return jsonify(gateway.get_job_history())

if __name__ == '__main__':
    app.run()

    with app.app_context():
        db.create_all()
