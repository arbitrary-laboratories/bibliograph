from flask import Flask, request
from alexandria.bq_gateway import BigQueryGateway
app = Flask(__name__)



@app.route("/")
def hello_world():
  return "Hello, World!"


if __name__ == '__main__':
    app.run(debug=True, port=5000)
