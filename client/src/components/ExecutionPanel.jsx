import React, { useState } from 'react'
import { useWorkflowStore } from '../store'

function OutputViewer({ data }) {
  const [expanded, setExpanded] = useState(true)
  
  if (data === null || data === undefined) {
    return <span style={{ color: '#888' }}>null</span>
  }
  
  if (typeof data === 'string') {
    return <span style={{ color: '#a5d6a7' }}>"{data}"</span>
  }
  
  if (typeof data === 'number' || typeof data === 'boolean') {
    return <span style={{ color: '#90caf9' }}>{String(data)}</span>
  }
  
  if (Array.isArray(data)) {
    if (data.length === 0) return <span>[]</span>
    return (
      <div style={{ marginLeft: '12px' }}>
        <span>[</span>
        {data.slice(0, 10).map((item, i) => (
          <div key={i} style={{ marginLeft: '12px' }}>
            <OutputViewer data={item} />
            {i < data.length - 1 && ','}
          </div>
        ))}
        {data.length > 10 && <div style={{ color: '#888', marginLeft: '12px' }}>... and {data.length - 10} more items</div>}
        <span>]</span>
      </div>
    )
  }
  
  if (typeof data === 'object') {
    const entries = Object.entries(data)
    if (entries.length === 0) return <span>{'{}'}</span>
    return (
      <div style={{ marginLeft: '12px' }}>
        <span>{'{'}</span>
        {entries.map(([key, value], i) => (
          <div key={key} style={{ marginLeft: '12px' }}>
            <span style={{ color: '#ce93d8' }}>"{key}"</span>: <OutputViewer data={value} />
            {i < entries.length - 1 && ','}
          </div>
        ))}
        <span>{'}'}</span>
      </div>
    )
  }
  
  return <span>{String(data)}</span>
}

function NodeResult({ nodeId, result }) {
  const [expanded, setExpanded] = useState(true)
  
  const statusColor = {
    success: '#4caf50',
    failed: '#f44336',
    skipped: '#ff9800'
  }[result.status] || '#888'
  
  return (
    <div style={{ 
      background: '#1e1e2e', 
      borderRadius: '8px', 
      marginBottom: '12px',
      border: `1px solid ${statusColor}30`
    }}>
      <div 
        onClick={() => setExpanded(!expanded)}
        style={{ 
          padding: '12px 16px', 
          cursor: 'pointer',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          borderBottom: expanded ? '1px solid #2d2d44' : 'none'
        }}
      >
        <div>
          <strong style={{ color: '#fff' }}>{nodeId}</strong>
          <span style={{ 
            marginLeft: '12px', 
            padding: '2px 8px', 
            borderRadius: '4px',
            fontSize: '12px',
            background: `${statusColor}30`,
            color: statusColor
          }}>
            {result.status}
          </span>
        </div>
        <span style={{ color: '#888' }}>{expanded ? '▼' : '▶'}</span>
      </div>
      
      {expanded && (
        <div style={{ padding: '12px 16px' }}>
          {result.error && (
            <div style={{ marginBottom: '12px' }}>
              <div style={{ color: '#f44336', fontWeight: 'bold', marginBottom: '4px' }}>Error:</div>
              <div style={{ color: '#ffcdd2', fontFamily: 'monospace', fontSize: '13px' }}>
                {result.error}
              </div>
            </div>
          )}
          
          {result.outputs && (
            <div>
              <div style={{ color: '#888', fontWeight: 'bold', marginBottom: '8px' }}>Output:</div>
              <div style={{ 
                fontFamily: 'monospace', 
                fontSize: '13px',
                background: '#12121a',
                padding: '12px',
                borderRadius: '4px',
                overflowX: 'auto'
              }}>
                <OutputViewer data={result.outputs} />
              </div>
            </div>
          )}
          
          {result.logs && (
            <div style={{ marginTop: '12px' }}>
              <div style={{ color: '#888', fontWeight: 'bold', marginBottom: '4px' }}>Logs:</div>
              <pre style={{ 
                color: '#aaa', 
                fontFamily: 'monospace', 
                fontSize: '12px',
                margin: 0,
                whiteSpace: 'pre-wrap'
              }}>
                {result.logs}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export default function ExecutionPanel() {
  const { executionStatus, executionLogs, nodeResults } = useWorkflowStore()

  if (!executionStatus && (!nodeResults || Object.keys(nodeResults).length === 0)) {
    return null
  }

  return (
    <div className="execution-panel" style={{ 
      background: '#16161e',
      borderTop: '1px solid #2d2d44',
      padding: '16px',
      maxHeight: '400px',
      overflowY: 'auto'
    }}>
      <h4 style={{ 
        margin: '0 0 16px 0',
        display: 'flex',
        alignItems: 'center',
        gap: '12px'
      }}>
        <span>Execution Results</span>
        {executionStatus === 'running' && (
          <span style={{ color: '#2196f3', fontSize: '14px' }}>Running...</span>
        )}
        {executionStatus === 'success' && (
          <span style={{ color: '#4caf50', fontSize: '14px' }}>Completed</span>
        )}
        {executionStatus === 'failed' && (
          <span style={{ color: '#f44336', fontSize: '14px' }}>Failed</span>
        )}
      </h4>
      
      {nodeResults && Object.entries(nodeResults).map(([nodeId, result]) => (
        <NodeResult key={nodeId} nodeId={nodeId} result={result} />
      ))}
      
      {(!nodeResults || Object.keys(nodeResults).length === 0) && executionLogs.length > 0 && (
        <div className="execution-log">
          {executionLogs.map((log, index) => (
            <div key={index} className={`log-entry ${log.type}`}>
              {log.nodeId && <strong>[{log.nodeId}] </strong>}
              {log.message}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
