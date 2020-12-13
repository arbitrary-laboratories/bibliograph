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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
