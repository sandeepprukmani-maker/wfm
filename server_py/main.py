import os
import time
from flask import Flask, send_from_directory, request
from flask_cors import CORS
from .models import init_db
from .utils import log
from .workflows import register_workflow_routes
from .airflow_routes import register_airflow_routes
from .management import register_management_routes

app = Flask(__name__, static_folder='../client/dist', static_url_path='')
CORS(app)

init_db()

@app.before_request
def log_request():
    request.start_time = time.time()

@app.after_request
def log_response(response):
    if hasattr(request, 'start_time') and request.path.startswith('/api'):
        duration = int((time.time() - request.start_time) * 1000)
        log(f"{request.method} {request.path} {response.status_code} in {duration}ms")
    return response

# Register module routes
register_workflow_routes(app)
register_airflow_routes(app)
register_management_routes(app)

@app.route('/')
@app.route('/<path:path>')
def serve_frontend(path=''):
    if path and os.path.exists(os.path.join(app.static_folder or '', path)):
        return send_from_directory(app.static_folder or '', path)
    if os.path.exists(os.path.join(app.static_folder or '', 'index.html')):
        return send_from_directory(app.static_folder or '', 'index.html')
    return "Frontend build not found. Please run 'npm run build' or check client/dist directory.", 404

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log(f"serving on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
