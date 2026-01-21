import React, { useEffect } from 'react'
import { useWorkflowStore } from './store'
import Sidebar from './components/Sidebar'
import Toolbar from './components/Toolbar'
import Canvas from './components/Canvas'
import NodePanel from './components/NodePanel'
import ExecutionPanel from './components/ExecutionPanel'

export default function App() {
  const { fetchWorkflows, selectedNode } = useWorkflowStore()

  useEffect(() => {
    fetchWorkflows()
  }, [fetchWorkflows])

  return (
    <div className="app-container">
      <Sidebar />
      <div className="main-content">
        <Toolbar />
        <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
            <Canvas />
            <ExecutionPanel />
          </div>
          <NodePanel />
        </div>
      </div>
    </div>
  )
}
