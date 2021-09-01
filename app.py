from chalice import Chalice
from chalicelib.functions import db, text

app = Chalice(app_name='hayden-api')
app.debug = True

@app.route("/test")
def auth():
    return db.scan("coconuts")

@app.route("/s/{table}")
def scan(table):
    return db.scan(table)

@app.route("/p/{table}")
def pscan(table):
    per_page = None
    if app.current_request.query_params is not None:
        per_page = app.current_request.query_params.get("per_page", None)
    return db.pscan(table, None, per_page)

@app.route("/q/{table}")
def qscan(table):
    return db.qscan(table, app.current_request.query_params)
    
@app.route("/p/{table}/{lastEvaluatedKey}")
def pscan(table, lastEvaluatedKey):
    per_page = None
    if app.current_request.query_params is not None:
        per_page = app.current_request.query_params.get("per_page", None)
    return db.pscan(table, lastEvaluatedKey, per_page)

@app.route("/{table}/post", methods=['POST'], content_types=["application/x-www-form-urlencoded", "application/json"])
def create(table):
    return db.post(table, text.get_request_data(app.current_request))

@app.route("/{table}/update", methods=['POST'], content_types=["application/x-www-form-urlencoded", "application/json"])
def update(table):
    return db.update(table, text.get_request_data(app.current_request))
    
@app.route("/{table}/delete", methods=['POST'], content_types=["application/x-www-form-urlencoded", "application/json"])
def delete(table):
    return db.delete(table, text.get_request_data(app.current_request))

@app.route("/{table}/{id}")
def get(table, id):
    return db.get(table, id)
