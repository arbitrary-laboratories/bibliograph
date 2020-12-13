from flask import Flask, request, jsonify
from alexandria.bq_gateway import BigQueryGateway
app = Flask(__name__)

@app.route('/tables', methods=['GET'])
def get_project_tables():
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    gateway = BigQueryGateway()
    tables = gateway.get_tables(project, dataset)
    return jsonify(tables)

@app.route('/table_metadata', methods=['GET'])
def get_metadata():
    project = request.args.get('project')
    dataset = request.args.get('dataset')
    table_name = request.args.get('tablename')
    gateway = BigQueryGateway()
    schema, num_rows = gateway.get_bq_table_metadata(project, dataset, table_name)
    return jsonify([gateway.serialize_schema(schema), num_rows])

@app.route('/query_history', methods=['GET'])
def get_qh():
    gateway = BigQueryGateway()
    return jsonify(gateway.get_query_history())



if __name__ == '__main__':
    app.run(debug=True, port=5000)
