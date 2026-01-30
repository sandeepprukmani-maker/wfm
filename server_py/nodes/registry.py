import os
import importlib
import pkgutil

class BaseNode:
    def __init__(self, config, context, logs, storage, execution_id):
        self.config = config
        self.context = context
        self.logs = logs
        self.storage = storage
        self.execution_id = execution_id

    def execute(self):
        raise NotImplementedError("Subclasses must implement execute()")

NODE_REGISTRY = {}

def register_node(node_type):
    def decorator(cls):
        NODE_REGISTRY[node_type] = cls
        return cls
    return decorator

def discover_nodes():
    nodes_path = os.path.dirname(__file__)
    for _, name, _ in pkgutil.iter_modules([nodes_path]):
        if name != 'registry':
            importlib.import_module(f'.{name}', package='server_py.nodes')

def get_node_class(node_type):
    if not NODE_REGISTRY:
        discover_nodes()
    return NODE_REGISTRY.get(node_type)
