import React from 'react'
import { useWorkflowStore } from '../store'

const nodeTypes = [
  { type: 'api', label: 'API Call', description: 'HTTP requests', icon: '🔗' },
  { type: 'sql', label: 'SQL Query', description: 'Database queries', icon: '📊' },
  { type: 'python', label: 'Python Code', description: 'Custom scripts', icon: '🐍' },
  { type: 'decision', label: 'Decision', description: 'Conditional branch', icon: '❓' },
  { type: 'wait', label: 'Wait', description: 'Delay/loop', icon: '⏱️' },
  { type: 'llm', label: 'LLM Generate', description: 'AI code generation', icon: '🤖' },
  { type: 'airflow', label: 'Airflow', description: 'DAG operations', icon: '🌀' }
]

export default function Sidebar() {
  const { workflows, currentWorkflow, createWorkflow, loadWorkflow, deleteWorkflow } = useWorkflowStore()

  const onDragStart = (event, nodeType) => {
    event.dataTransfer.setData('application/reactflow', nodeType)
    event.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h1>Workflow Builder</h1>
        <p>Visual workflow orchestration</p>
      </div>

      <div className="node-palette">
        <h3>Nodes</h3>
        {nodeTypes.map((node) => (
          <div
            key={node.type}
            className="node-item"
            draggable
            onDragStart={(e) => onDragStart(e, node.type)}
          >
            <div className={`node-icon ${node.type}`}>{node.icon}</div>
            <div className="node-info">
              <h4>{node.label}</h4>
              <p>{node.description}</p>
            </div>
          </div>
        ))}
      </div>

      <div className="workflow-list">
        <h3>Workflows</h3>
        <button
          className="btn btn-primary"
          style={{ width: '100%', marginBottom: '12px' }}
          onClick={() => createWorkflow()}
        >
          + New Workflow
        </button>
        {workflows.map((workflow) => (
          <div
            key={workflow.id}
            className={`workflow-item ${currentWorkflow?.id === workflow.id ? 'active' : ''}`}
            onClick={() => loadWorkflow(workflow.id)}
          >
            <h4>{workflow.name}</h4>
            <p>{workflow.nodes?.length || 0} nodes</p>
          </div>
        ))}
      </div>
    </div>
  )
}
