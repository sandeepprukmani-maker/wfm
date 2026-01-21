import json
from datetime import datetime
from collections import defaultdict, deque
from typing import Optional
from server.models import db, Workflow, WorkflowExecution, NodeExecution
from server.node_executor import NodeExecutor


class WorkflowEngine:
    """Executes workflows with output-aware dependency management."""
    
    def __init__(self):
        self.executor = NodeExecutor()
    
    def execute_workflow(self, workflow_id: int) -> dict:
        """Execute a complete workflow."""
        workflow = Workflow.query.get(workflow_id)
        if not workflow:
            return {'error': 'Workflow not found'}
        
        execution = WorkflowExecution(
            workflow_id=workflow_id,
            status='running',
            started_at=datetime.utcnow()
        )
        db.session.add(execution)
        db.session.commit()
        
        try:
            nodes = json.loads(workflow.nodes) if workflow.nodes else []
            edges = json.loads(workflow.edges) if workflow.edges else []
            
            result = self._execute_dag(execution.id, nodes, edges)
            
            execution.status = 'success' if not result.get('error') else 'failed'
            execution.completed_at = datetime.utcnow()
            execution.outputs = json.dumps(result.get('outputs', {}))
            execution.error = result.get('error')
            db.session.commit()
            
            return {
                'execution_id': execution.id,
                'status': execution.status,
                'outputs': result.get('outputs', {}),
                'node_results': result.get('node_results', {})
            }
        
        except Exception as e:
            execution.status = 'failed'
            execution.completed_at = datetime.utcnow()
            execution.error = str(e)
            db.session.commit()
            return {'error': str(e), 'execution_id': execution.id}
    
    def _detect_cycle(self, nodes: list, edges: list) -> bool:
        """Detect if the graph has a cycle using Kahn's algorithm."""
        node_ids = {node['id'] for node in nodes}
        in_degree = {nid: 0 for nid in node_ids}
        adjacency = defaultdict(list)
        
        for edge in edges:
            source = edge.get('source')
            target = edge.get('target')
            if source and target and source in node_ids and target in node_ids:
                adjacency[source].append(target)
                in_degree[target] += 1
        
        queue = deque([nid for nid in node_ids if in_degree[nid] == 0])
        visited = 0
        
        while queue:
            node_id = queue.popleft()
            visited += 1
            for neighbor in adjacency[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return visited < len(node_ids)
    
    def _execute_dag(self, execution_id: int, nodes: list, edges: list) -> dict:
        """Execute nodes in DAG order with dependency handling."""
        if self._detect_cycle(nodes, edges):
            return {
                'error': 'Workflow contains a cycle. Cyclic workflows are not supported.',
                'outputs': {},
                'node_results': {}
            }
        
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
        
        queue = deque([nid for nid in node_map if in_degree[nid] == 0])
        
        context = {}
        node_results = {}
        max_iterations = 1000
        iteration = 0
        
        while queue and iteration < max_iterations:
            iteration += 1
            node_id = queue.popleft()
            node = node_map.get(node_id)
            
            if not node:
                continue
            
            node_exec = NodeExecution(
                execution_id=execution_id,
                node_id=node_id,
                node_type=node.get('type', 'unknown'),
                status='running',
                started_at=datetime.utcnow(),
                inputs=json.dumps(context)
            )
            db.session.add(node_exec)
            db.session.commit()
            
            result = self.executor.execute_node(node, context)
            
            node_exec.status = result.get('status', 'failed')
            node_exec.completed_at = datetime.utcnow()
            node_exec.outputs = json.dumps(result.get('outputs', {}))
            node_exec.logs = result.get('logs', '')
            node_exec.error = result.get('error')
            db.session.commit()
            
            node_results[node_id] = result
            
            if result.get('status') == 'success':
                context[node_id] = result.get('outputs', {})
            
            if result.get('status') == 'failed':
                return {
                    'error': f"Node {node_id} failed: {result.get('error')}",
                    'outputs': context,
                    'node_results': node_results
                }
            
            if node.get('type') == 'decision':
                branch = result.get('outputs', {}).get('branch', 'true')
                for edge in edges:
                    if edge.get('source') == node_id:
                        target = edge.get('target')
                        edge_label = edge.get('sourceHandle', edge.get('label', ''))
                        
                        if branch == 'true' and edge_label in ['true', 'yes', 'success', '']:
                            in_degree[target] -= 1
                            if in_degree[target] == 0:
                                queue.append(target)
                        elif branch == 'false' and edge_label in ['false', 'no', 'failure']:
                            in_degree[target] -= 1
                            if in_degree[target] == 0:
                                queue.append(target)
            else:
                for next_node in adjacency[node_id]:
                    in_degree[next_node] -= 1
                    if in_degree[next_node] == 0:
                        queue.append(next_node)
        
        executed_count = len(node_results)
        total_nodes = len(nodes)
        
        if executed_count < total_nodes:
            skipped = [nid for nid in node_map if nid not in node_results]
            return {
                'outputs': context,
                'node_results': node_results,
                'warning': f'{len(skipped)} nodes were not executed (unreachable or skipped by decision): {skipped[:5]}'
            }
        
        return {
            'outputs': context,
            'node_results': node_results
        }
    
    def get_execution_status(self, execution_id: int) -> Optional[dict]:
        """Get the status of a workflow execution."""
        execution = WorkflowExecution.query.get(execution_id)
        if not execution:
            return None
        
        node_execs = NodeExecution.query.filter_by(execution_id=execution_id).all()
        
        return {
            'execution': execution.to_dict(),
            'node_executions': [n.to_dict() for n in node_execs]
        }
