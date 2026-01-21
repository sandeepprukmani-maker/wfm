import os
import json
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from server.models import db, Workflow, WorkflowExecution, NodeExecution
from server.workflow_engine import WorkflowEngine
from server.exporter import export_to_python, export_to_yaml
from server.llm_service import generate_code, analyze_output

app = Flask(__name__, static_folder='../client/dist', static_url_path='')
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key')

CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///workflow.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

with app.app_context():
    db.create_all()

engine = WorkflowEngine()


@app.after_request
def add_cache_headers(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route('/')
def serve_frontend():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/<path:path>')
def serve_static(path):
    if os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/api/workflows', methods=['GET'])
def list_workflows():
    workflows = Workflow.query.order_by(Workflow.updated_at.desc()).all()
    return jsonify([w.to_dict() for w in workflows])


@app.route('/api/workflows', methods=['POST'])
def create_workflow():
    data = request.json
    workflow = Workflow(
        name=data.get('name', 'Untitled Workflow'),
        description=data.get('description', ''),
        nodes=json.dumps(data.get('nodes', [])),
        edges=json.dumps(data.get('edges', []))
    )
    db.session.add(workflow)
    db.session.commit()
    return jsonify(workflow.to_dict()), 201


@app.route('/api/workflows/<int:workflow_id>', methods=['GET'])
def get_workflow(workflow_id):
    workflow = Workflow.query.get_or_404(workflow_id)
    return jsonify(workflow.to_dict())


@app.route('/api/workflows/<int:workflow_id>', methods=['PUT'])
def update_workflow(workflow_id):
    workflow = Workflow.query.get_or_404(workflow_id)
    data = request.json
    
    if 'name' in data:
        workflow.name = data['name']
    if 'description' in data:
        workflow.description = data['description']
    if 'nodes' in data:
        workflow.nodes = json.dumps(data['nodes'])
    if 'edges' in data:
        workflow.edges = json.dumps(data['edges'])
    
    db.session.commit()
    return jsonify(workflow.to_dict())


@app.route('/api/workflows/<int:workflow_id>', methods=['DELETE'])
def delete_workflow(workflow_id):
    workflow = Workflow.query.get_or_404(workflow_id)
    db.session.delete(workflow)
    db.session.commit()
    return '', 204


@app.route('/api/workflows/<int:workflow_id>/execute', methods=['POST'])
def execute_workflow(workflow_id):
    result = engine.execute_workflow(workflow_id)
    return jsonify(result)


@app.route('/api/workflows/<int:workflow_id>/export/python', methods=['GET'])
def export_workflow_python(workflow_id):
    workflow = Workflow.query.get_or_404(workflow_id)
    code = export_to_python(workflow.to_dict())
    return code, 200, {'Content-Type': 'text/plain'}


@app.route('/api/workflows/<int:workflow_id>/export/yaml', methods=['GET'])
def export_workflow_yaml(workflow_id):
    workflow = Workflow.query.get_or_404(workflow_id)
    yaml_content = export_to_yaml(workflow.to_dict())
    return yaml_content, 200, {'Content-Type': 'text/yaml'}


@app.route('/api/executions/<int:execution_id>', methods=['GET'])
def get_execution(execution_id):
    result = engine.get_execution_status(execution_id)
    if not result:
        return jsonify({'error': 'Execution not found'}), 404
    return jsonify(result)


@app.route('/api/workflows/<int:workflow_id>/executions', methods=['GET'])
def list_workflow_executions(workflow_id):
    executions = WorkflowExecution.query.filter_by(workflow_id=workflow_id).order_by(
        WorkflowExecution.started_at.desc()
    ).limit(50).all()
    return jsonify([e.to_dict() for e in executions])


@app.route('/api/llm/generate', methods=['POST'])
def llm_generate():
    data = request.json
    prompt = data.get('prompt', '')
    code_type = data.get('code_type', 'python')
    context = data.get('context', {})
    
    try:
        result = generate_code(prompt, code_type, context)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/llm/analyze', methods=['POST'])
def llm_analyze():
    data = request.json
    output = data.get('output', '')
    analysis_prompt = data.get('prompt', '')
    
    try:
        result = analyze_output(output, analysis_prompt)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
