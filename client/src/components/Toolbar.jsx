import React, { useState } from 'react'
import { useWorkflowStore } from '../store'

export default function Toolbar() {
  const {
    currentWorkflow,
    updateWorkflowName,
    saveWorkflow,
    executeWorkflow,
    exportToPython,
    executionStatus
  } = useWorkflowStore()
  
  const [showExport, setShowExport] = useState(false)
  const [exportCode, setExportCode] = useState('')

  const handleExport = async () => {
    const code = await exportToPython()
    if (code) {
      setExportCode(code)
      setShowExport(true)
    }
  }

  const handleCopyCode = () => {
    navigator.clipboard.writeText(exportCode)
  }

  if (!currentWorkflow) {
    return (
      <div className="toolbar">
        <div className="toolbar-left">
          <span style={{ color: '#666' }}>Create or select a workflow</span>
        </div>
      </div>
    )
  }

  return (
    <>
      <div className="toolbar">
        <div className="toolbar-left">
          <input
            className="workflow-name-input"
            type="text"
            value={currentWorkflow.name}
            onChange={(e) => updateWorkflowName(e.target.value)}
            placeholder="Workflow name"
          />
        </div>
        <div className="toolbar-right">
          <button className="btn btn-secondary" onClick={saveWorkflow}>
            Save
          </button>
          <button className="btn btn-secondary" onClick={handleExport}>
            Export Python
          </button>
          <button
            className="btn btn-success"
            onClick={executeWorkflow}
            disabled={executionStatus === 'running'}
          >
            {executionStatus === 'running' ? 'Running...' : 'Execute'}
          </button>
        </div>
      </div>

      {showExport && (
        <div className="modal-overlay" onClick={() => setShowExport(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <h2>Export to Python</h2>
            <pre className="code-preview">{exportCode}</pre>
            <div className="modal-actions">
              <button className="btn btn-secondary" onClick={handleCopyCode}>
                Copy Code
              </button>
              <button className="btn btn-primary" onClick={() => setShowExport(false)}>
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
