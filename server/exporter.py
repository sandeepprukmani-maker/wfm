import json
from collections import defaultdict, deque


def export_to_python(workflow: dict) -> str:
    """Export a workflow to executable Python code."""
    nodes = workflow.get('nodes', [])
    edges = workflow.get('edges', [])
    
    node_map = {node['id']: node for node in nodes}
    adjacency = defaultdict(list)
    in_degree = defaultdict(int)
    
    for node in nodes:
        in_degree[node['id']] = 0
    
    for edge in edges:
        source = edge.get('source')
        target = edge.get('target')
        if source and target:
            adjacency[source].append(target)
            in_degree[target] += 1
    
    sorted_nodes = []
    queue = deque([nid for nid in node_map if in_degree[nid] == 0])
    
    while queue:
        node_id = queue.popleft()
        sorted_nodes.append(node_map[node_id])
        for next_id in adjacency[node_id]:
            in_degree[next_id] -= 1
            if in_degree[next_id] == 0:
                queue.append(next_id)
    
    code_lines = [
        '"""',
        f'Auto-generated workflow: {workflow.get("name", "Unnamed")}',
        f'Description: {workflow.get("description", "")}',
        '"""',
        '',
        'import time',
        'import json',
        'import sqlite3',
        'import requests',
        '',
        '',
        'class WorkflowContext:',
        '    """Stores outputs from each node for use in downstream nodes."""',
        '    def __init__(self):',
        '        self.data = {}',
        '',
        '    def set(self, node_id: str, outputs: dict):',
        '        self.data[node_id] = outputs',
        '',
        '    def get(self, node_id: str, key: str = None):',
        '        if key:',
        '            return self.data.get(node_id, {}).get(key)',
        '        return self.data.get(node_id, {})',
        '',
        '',
        'def run_workflow():',
        '    """Execute the workflow."""',
        '    ctx = WorkflowContext()',
        ''
    ]
    
    for node in sorted_nodes:
        node_id = node['id']
        node_type = node.get('type', '')
        node_data = node.get('data', {})
        label = node_data.get('label', node_id)
        
        code_lines.append(f'    # Node: {label} ({node_type})')
        code_lines.append(f'    print("Executing node: {label}")')
        
        if node_type == 'api':
            url = node_data.get('url', '')
            method = node_data.get('method', 'GET')
            code_lines.extend([
                f'    response = requests.{method.lower()}("{url}")',
                f'    ctx.set("{node_id}", {{"response": response.json(), "status_code": response.status_code}})',
                ''
            ])
        
        elif node_type == 'sql':
            query = node_data.get('query', '').replace("'", "\\'")
            code_lines.extend([
                f'    conn = sqlite3.connect("workflow.db")',
                f'    conn.row_factory = sqlite3.Row',
                f'    cursor = conn.cursor()',
                f'    cursor.execute(\'{query}\')',
                f'    rows = [dict(row) for row in cursor.fetchall()]',
                f'    ctx.set("{node_id}", {{"rows": rows, "row_count": len(rows)}})',
                f'    conn.close()',
                ''
            ])
        
        elif node_type == 'python':
            code = node_data.get('code', '')
            code_lines.extend([
                f'    # Custom Python code',
                f'    result = {{}}',
            ])
            for line in code.split('\n'):
                code_lines.append(f'    {line}')
            code_lines.extend([
                f'    ctx.set("{node_id}", result)',
                ''
            ])
        
        elif node_type == 'decision':
            condition = node_data.get('condition', 'True')
            code_lines.extend([
                f'    decision_result = {condition}',
                f'    ctx.set("{node_id}", {{"result": decision_result, "branch": "true" if decision_result else "false"}})',
                ''
            ])
        
        elif node_type == 'wait':
            seconds = node_data.get('seconds', 10)
            code_lines.extend([
                f'    time.sleep({seconds})',
                f'    ctx.set("{node_id}", {{"waited": {seconds}}})',
                ''
            ])
        
        elif node_type == 'llm':
            prompt = node_data.get('prompt', '').replace('"', '\\"')
            code_type = node_data.get('code_type', 'python')
            code_lines.extend([
                f'    # LLM Node - Prompt: {prompt[:50]}...',
                f'    # Note: Requires OpenAI API to generate {code_type} code',
                f'    # Implement LLM call here or use pre-generated code',
                f'    ctx.set("{node_id}", {{"status": "requires_llm_integration"}})',
                ''
            ])
        
        elif node_type == 'airflow':
            action = node_data.get('action', 'trigger')
            dag_id = node_data.get('dag_id', '')
            code_lines.extend([
                f'    # Airflow {action} for DAG: {dag_id}',
                f'    # Implement Airflow API call here',
                f'    ctx.set("{node_id}", {{"action": "{action}", "dag_id": "{dag_id}"}})',
                ''
            ])
    
    code_lines.extend([
        '    print("Workflow completed!")',
        '    return ctx.data',
        '',
        '',
        'if __name__ == "__main__":',
        '    results = run_workflow()',
        '    print(json.dumps(results, indent=2, default=str))',
    ])
    
    return '\n'.join(code_lines)


def export_to_yaml(workflow: dict) -> str:
    """Export a workflow to YAML format."""
    import yaml
    
    export_data = {
        'name': workflow.get('name', 'Unnamed'),
        'description': workflow.get('description', ''),
        'version': '1.0',
        'nodes': [],
        'edges': []
    }
    
    for node in workflow.get('nodes', []):
        export_data['nodes'].append({
            'id': node.get('id'),
            'type': node.get('type'),
            'label': node.get('data', {}).get('label', ''),
            'config': node.get('data', {})
        })
    
    for edge in workflow.get('edges', []):
        export_data['edges'].append({
            'source': edge.get('source'),
            'target': edge.get('target'),
            'label': edge.get('label', '')
        })
    
    try:
        import yaml
        return yaml.dump(export_data, default_flow_style=False)
    except ImportError:
        return json.dumps(export_data, indent=2)
