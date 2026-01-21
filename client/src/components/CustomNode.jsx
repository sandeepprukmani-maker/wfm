import React, { memo } from 'react'
import { Handle, Position } from 'reactflow'

const nodeColors = {
  api: '#3b82f6',
  sql: '#10b981',
  python: '#f59e0b',
  decision: '#8b5cf6',
  wait: '#6b7280',
  llm: '#ec4899',
  airflow: '#06b6d4'
}

const nodeIcons = {
  api: '🔗',
  sql: '📊',
  python: '🐍',
  decision: '❓',
  wait: '⏱️',
  llm: '🤖',
  airflow: '🌀'
}

function CustomNode({ data, type, selected }) {
  const color = nodeColors[type] || '#666'
  const icon = nodeIcons[type] || '📦'
  
  const getPreview = () => {
    switch (type) {
      case 'api':
        return data.url ? `${data.method} ${data.url.substring(0, 30)}...` : 'Configure URL'
      case 'sql':
        return data.query ? data.query.substring(0, 40) + '...' : 'Configure query'
      case 'python':
        return data.code ? data.code.substring(0, 40) + '...' : 'Add code'
      case 'decision':
        return data.condition || 'Set condition'
      case 'wait':
        return `Wait ${data.seconds || 10} seconds`
      case 'llm':
        return data.prompt ? data.prompt.substring(0, 40) + '...' : 'Enter prompt'
      case 'airflow':
        return data.dag_id ? `${data.action}: ${data.dag_id}` : 'Configure DAG'
      default:
        return ''
    }
  }

  return (
    <div className={`custom-node ${selected ? 'selected' : ''}`}>
      <Handle type="target" position={Position.Top} />
      
      <div className="node-header">
        <div className="node-type-icon" style={{ background: color }}>
          {icon}
        </div>
        <span className="node-label">{data.label || type}</span>
        <span className="node-type-badge">{type}</span>
      </div>
      
      <div className="node-content">
        {getPreview()}
      </div>
      
      {type === 'decision' ? (
        <>
          <Handle
            type="source"
            position={Position.Bottom}
            id="true"
            style={{ left: '30%', background: '#10b981' }}
          />
          <Handle
            type="source"
            position={Position.Bottom}
            id="false"
            style={{ left: '70%', background: '#ef4444' }}
          />
        </>
      ) : (
        <Handle type="source" position={Position.Bottom} />
      )}
    </div>
  )
}

export default memo(CustomNode)
