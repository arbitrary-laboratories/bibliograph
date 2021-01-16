from exabyte import settings

from flask import Flask, request, jsonify
from flask_cors import CORS
from .db_client import DbClient
from exabyte.models.database import init_db
from exabyte.models.main import db


def create_app(config=settings):
    app = Flask(__name__)
    app.config.from_object(config)
    init_db()

    db.init_app(app)

    # TODO: blueprint style view initialization
    #init_views(app)

    return app

# TODO: wrap in manager function
app = create_app()

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


if __name__ == '__main__':
    app.run()

    with app.app_context():
        db.create_all()
