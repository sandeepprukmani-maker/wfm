import React, { useState } from 'react'
import { useWorkflowStore } from '../store'

function parseCurl(curlCommand) {
  const result = { method: 'GET', url: '', headers: {}, body: '' }
  
  const urlMatch = curlCommand.match(/curl\s+(?:--request\s+\w+\s+)?(?:--location\s+)?['"]?(https?:\/\/[^\s'"]+)['"]?/)
    || curlCommand.match(/['"]?(https?:\/\/[^\s'"]+)['"]?/)
  if (urlMatch) {
    result.url = urlMatch[1]
  }
  
  const methodMatch = curlCommand.match(/-X\s+(\w+)|--request\s+(\w+)/)
  if (methodMatch) {
    result.method = (methodMatch[1] || methodMatch[2]).toUpperCase()
  } else if (curlCommand.includes('--data') || curlCommand.includes('-d ')) {
    result.method = 'POST'
  }
  
  const headerRegex = /-H\s+['"]([^'"]+)['"]/g
  let headerMatch
  while ((headerMatch = headerRegex.exec(curlCommand)) !== null) {
    const [key, ...valueParts] = headerMatch[1].split(':')
    if (key && valueParts.length > 0) {
      result.headers[key.trim()] = valueParts.join(':').trim()
    }
  }
  
  const dataMatch = curlCommand.match(/(?:--data-raw|--data|-d)\s+['"](.+?)['"]\s*(?=-|$)/s)
    || curlCommand.match(/(?:--data-raw|--data|-d)\s+'([^']+)'/)
    || curlCommand.match(/(?:--data-raw|--data|-d)\s+"([^"]+)"/)
  if (dataMatch) {
    result.body = dataMatch[1]
  }
  
  return result
}

const NODE_OUTPUT_FIELDS = {
  api: ['status_code', 'response', 'headers'],
  sql: ['rows', 'row_count'],
  python: ['result'],
  decision: ['result', 'branch'],
  wait: ['waited'],
  llm: ['code', 'explanation'],
  airflow: ['dag_run_id', 'state', 'logs']
}

function VariablePicker({ nodes, currentNodeId, onInsert }) {
  const [expanded, setExpanded] = useState(null)
  
  const otherNodes = nodes.filter(n => n.id !== currentNodeId)
  
  if (otherNodes.length === 0) {
    return (
      <div style={{ padding: '12px', background: '#1e1e2e', borderRadius: '6px', marginBottom: '16px' }}>
        <p style={{ color: '#888', fontSize: '12px', margin: 0 }}>
          Add more nodes to reference their outputs
        </p>
      </div>
    )
  }
  
  return (
    <div style={{ background: '#1e1e2e', borderRadius: '6px', marginBottom: '16px', padding: '12px' }}>
      <p style={{ color: '#aaa', fontSize: '12px', marginBottom: '8px', fontWeight: 'bold' }}>
        Click to insert variable reference:
      </p>
      {otherNodes.map(node => {
        const nodeLabel = node.data?.label || node.type
        const fields = NODE_OUTPUT_FIELDS[node.type] || ['output']
        const isExpanded = expanded === node.id
        
        return (
          <div key={node.id} style={{ marginBottom: '4px' }}>
            <button
              onClick={() => setExpanded(isExpanded ? null : node.id)}
              style={{
                width: '100%',
                textAlign: 'left',
                background: '#2d2d44',
                border: 'none',
                color: '#fff',
                padding: '8px 12px',
                borderRadius: '4px',
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center'
              }}
            >
              <span>
                <strong>{nodeLabel}</strong>
                <span style={{ color: '#888', marginLeft: '8px', fontSize: '12px' }}>({node.id})</span>
              </span>
              <span style={{ color: '#888' }}>{isExpanded ? '▼' : '▶'}</span>
            </button>
            {isExpanded && (
              <div style={{ paddingLeft: '12px', paddingTop: '4px' }}>
                {fields.map(field => (
                  <button
                    key={field}
                    onClick={() => onInsert(`{{ ${node.id}.${field} }}`)}
                    style={{
                      display: 'block',
                      width: '100%',
                      textAlign: 'left',
                      background: 'transparent',
                      border: 'none',
                      color: '#4fc3f7',
                      padding: '6px 8px',
                      cursor: 'pointer',
                      fontFamily: 'monospace',
                      fontSize: '13px',
                      borderRadius: '4px'
                    }}
                    onMouseOver={e => e.target.style.background = '#2d2d44'}
                    onMouseOut={e => e.target.style.background = 'transparent'}
                  >
                    {`{{ ${node.id}.${field} }}`}
                  </button>
                ))}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

export default function NodePanel() {
  const { selectedNode, updateNode, deleteNode, generateWithLLM, nodes } = useWorkflowStore()
  const [generating, setGenerating] = useState(false)
  const [curlInput, setCurlInput] = useState('')
  const [copiedVar, setCopiedVar] = useState(null)

  if (!selectedNode) {
    return (
      <div className="panel-right">
        <div className="panel-header">
          <h3>Node Properties</h3>
        </div>
        <div className="panel-content">
          <div className="empty-state">
            <p>Select a node to edit its properties</p>
          </div>
        </div>
      </div>
    )
  }

  const { type, data, id } = selectedNode

  const handleChange = (field, value) => {
    updateNode(id, { [field]: value })
  }

  const handleGenerateCode = async () => {
    if (!data.prompt) return
    setGenerating(true)
    try {
      const result = await generateWithLLM(data.prompt, data.code_type || 'python')
      if (result.code) {
        if (data.code_type === 'sql') {
          updateNode(id, { generated_code: result.code, explanation: result.explanation })
        } else {
          updateNode(id, { generated_code: result.code, explanation: result.explanation })
        }
      }
    } finally {
      setGenerating(false)
    }
  }

  const renderFields = () => {
    switch (type) {
      case 'api':
        const handleParseCurl = () => {
          if (!curlInput.trim()) return
          const parsed = parseCurl(curlInput)
          updateNode(id, {
            url: parsed.url,
            method: parsed.method,
            headers: parsed.headers,
            body: parsed.body
          })
          setCurlInput('')
        }
        
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Paste cURL Command</label>
              <textarea
                value={curlInput}
                onChange={(e) => setCurlInput(e.target.value)}
                placeholder="curl -X POST https://api.example.com -H 'Content-Type: application/json' -d '{key: value}'"
                style={{ minHeight: '80px' }}
              />
              <button
                className="btn btn-primary"
                onClick={handleParseCurl}
                style={{ width: '100%', marginTop: '8px' }}
                disabled={!curlInput.trim()}
              >
                Parse cURL
              </button>
            </div>
            <hr style={{ border: 'none', borderTop: '1px solid #2d2d44', margin: '16px 0' }} />
            <div className="form-group">
              <label>URL</label>
              <input
                type="text"
                value={data.url || ''}
                onChange={(e) => handleChange('url', e.target.value)}
                placeholder="https://api.example.com/endpoint"
              />
            </div>
            <div className="form-group">
              <label>Method</label>
              <select
                value={data.method || 'GET'}
                onChange={(e) => handleChange('method', e.target.value)}
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="DELETE">DELETE</option>
                <option value="PATCH">PATCH</option>
              </select>
            </div>
            <div className="form-group">
              <label>Headers (JSON)</label>
              <textarea
                value={typeof data.headers === 'object' ? JSON.stringify(data.headers, null, 2) : data.headers || '{}'}
                onChange={(e) => {
                  try {
                    handleChange('headers', JSON.parse(e.target.value))
                  } catch {
                    handleChange('headers', e.target.value)
                  }
                }}
                placeholder='{"Authorization": "Bearer token"}'
              />
            </div>
            <div className="form-group">
              <label>Body (JSON)</label>
              <textarea
                value={data.body || ''}
                onChange={(e) => handleChange('body', e.target.value)}
                placeholder='{"key": "value"}'
              />
            </div>
          </>
        )

      case 'sql':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Database Path</label>
              <input
                type="text"
                value={data.database || 'workflow.db'}
                onChange={(e) => handleChange('database', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>SQL Query</label>
              <textarea
                value={data.query || ''}
                onChange={(e) => handleChange('query', e.target.value)}
                placeholder="SELECT * FROM table WHERE id = {{ node_id.value }}"
                style={{ minHeight: '150px' }}
              />
            </div>
            <p style={{ fontSize: '11px', color: '#666', marginTop: '-10px' }}>
              Use {'{{ node_id.field }}'} to reference outputs from other nodes
            </p>
          </>
        )

      case 'python':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Python Code</label>
              <textarea
                value={data.code || ''}
                onChange={(e) => handleChange('code', e.target.value)}
                placeholder="# Access previous outputs via context dict\n# Store results in 'result' dict\nresult = {'value': context.get('prev_node', {}).get('data')}"
                style={{ minHeight: '200px' }}
              />
            </div>
            <p style={{ fontSize: '11px', color: '#666', marginTop: '-10px' }}>
              Use <code>context</code> to access outputs. Set <code>result</code> to pass data.
            </p>
          </>
        )

      case 'decision':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Condition</label>
              <textarea
                value={data.condition || ''}
                onChange={(e) => handleChange('condition', e.target.value)}
                placeholder="node_id.row_count > 0"
              />
            </div>
            <p style={{ fontSize: '11px', color: '#666', marginTop: '-10px' }}>
              Evaluates to True/False. Green = True, Red = False branch.
            </p>
          </>
        )

      case 'wait':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Wait Duration (seconds)</label>
              <input
                type="number"
                value={data.seconds || 10}
                onChange={(e) => handleChange('seconds', parseInt(e.target.value) || 10)}
                min="1"
              />
            </div>
          </>
        )

      case 'llm':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Code Type</label>
              <select
                value={data.code_type || 'python'}
                onChange={(e) => handleChange('code_type', e.target.value)}
              >
                <option value="python">Python</option>
                <option value="sql">SQL</option>
              </select>
            </div>
            <div className="form-group">
              <label>Prompt (describe what you need)</label>
              <textarea
                value={data.prompt || ''}
                onChange={(e) => handleChange('prompt', e.target.value)}
                placeholder="Write a query to get all orders from the last 7 days grouped by customer"
                style={{ minHeight: '120px' }}
              />
            </div>
            <div className="form-group">
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={data.execute_code || false}
                  onChange={(e) => handleChange('execute_code', e.target.checked)}
                />
                Execute generated code
              </label>
            </div>
            <button
              className="btn btn-primary"
              onClick={handleGenerateCode}
              disabled={generating || !data.prompt}
              style={{ width: '100%', marginBottom: '12px' }}
            >
              {generating ? 'Generating...' : 'Generate Code'}
            </button>
            {data.generated_code && (
              <div className="form-group">
                <label>Generated Code</label>
                <textarea
                  value={data.generated_code}
                  readOnly
                  style={{ minHeight: '150px', background: '#0f0f23' }}
                />
                {data.explanation && (
                  <p style={{ fontSize: '11px', color: '#888', marginTop: '8px' }}>
                    {data.explanation}
                  </p>
                )}
              </div>
            )}
          </>
        )

      case 'airflow':
        return (
          <>
            <div className="form-group">
              <label>Label</label>
              <input
                type="text"
                value={data.label || ''}
                onChange={(e) => handleChange('label', e.target.value)}
              />
            </div>
            <div className="form-group">
              <label>Action</label>
              <select
                value={data.action || 'trigger'}
                onChange={(e) => handleChange('action', e.target.value)}
              >
                <option value="trigger">Trigger DAG</option>
                <option value="status">Check Status</option>
                <option value="logs">Fetch Logs</option>
              </select>
            </div>
            <div className="form-group">
              <label>Airflow Base URL</label>
              <input
                type="text"
                value={data.base_url || ''}
                onChange={(e) => handleChange('base_url', e.target.value)}
                placeholder="https://airflow.example.com"
              />
            </div>
            <div className="form-group">
              <label>DAG ID</label>
              <input
                type="text"
                value={data.dag_id || ''}
                onChange={(e) => handleChange('dag_id', e.target.value)}
                placeholder="my_dag"
              />
            </div>
            {(data.action === 'status' || data.action === 'logs') && (
              <div className="form-group">
                <label>Run ID (use {'{{ node_id.dag_run_id }}'})</label>
                <input
                  type="text"
                  value={data.run_id || ''}
                  onChange={(e) => handleChange('run_id', e.target.value)}
                  placeholder="{{ trigger_node.dag_run_id }}"
                />
              </div>
            )}
            <div className="form-group">
              <label>Username</label>
              <input
                type="text"
                value={data.auth?.username || ''}
                onChange={(e) => handleChange('auth', { ...data.auth, username: e.target.value })}
              />
            </div>
            <div className="form-group">
              <label>Password</label>
              <input
                type="password"
                value={data.auth?.password || ''}
                onChange={(e) => handleChange('auth', { ...data.auth, password: e.target.value })}
              />
            </div>
          </>
        )

      default:
        return <p>Unknown node type</p>
    }
  }

  const copyNodeId = () => {
    navigator.clipboard.writeText(id)
  }

  const handleInsertVariable = (variable) => {
    navigator.clipboard.writeText(variable)
    setCopiedVar(variable)
    setTimeout(() => setCopiedVar(null), 2000)
  }

  return (
    <div className="panel-right">
      <div className="panel-header">
        <h3>Node Properties</h3>
        <button className="btn btn-danger" onClick={() => deleteNode(id)}>
          Delete
        </button>
      </div>
      <div className="panel-content">
        <div className="node-id-display" style={{ 
          background: '#1e1e2e', 
          padding: '8px 12px', 
          borderRadius: '6px', 
          marginBottom: '16px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <div>
            <span style={{ color: '#888', fontSize: '12px' }}>Node ID:</span>
            <code style={{ 
              color: '#4fc3f7', 
              marginLeft: '8px', 
              fontFamily: 'monospace',
              fontSize: '14px'
            }}>{id}</code>
          </div>
          <button 
            onClick={copyNodeId}
            style={{
              background: '#2d2d44',
              border: 'none',
              color: '#aaa',
              padding: '4px 8px',
              borderRadius: '4px',
              cursor: 'pointer',
              fontSize: '12px'
            }}
          >
            Copy
          </button>
        </div>
        {copiedVar && (
          <div style={{ 
            background: '#2d7d46', 
            color: '#fff', 
            padding: '8px 12px', 
            borderRadius: '6px', 
            marginBottom: '12px',
            fontSize: '13px'
          }}>
            Copied: <code>{copiedVar}</code>
          </div>
        )}
        <VariablePicker 
          nodes={nodes} 
          currentNodeId={id} 
          onInsert={handleInsertVariable} 
        />
        {renderFields()}
      </div>
    </div>
  )
}
