import React, { useCallback, useRef } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
  ReactFlowProvider
} from 'reactflow'
import 'reactflow/dist/style.css'

import { useWorkflowStore } from '../store'
import CustomNode from './CustomNode'

const nodeTypes = {
  api: CustomNode,
  sql: CustomNode,
  python: CustomNode,
  decision: CustomNode,
  wait: CustomNode,
  llm: CustomNode,
  airflow: CustomNode
}

function FlowCanvas() {
  const reactFlowWrapper = useRef(null)
  const {
    nodes,
    edges,
    setNodes,
    setEdges,
    addNode,
    selectNode,
    currentWorkflow
  } = useWorkflowStore()

  const [localNodes, setLocalNodes, onNodesChange] = useNodesState(nodes)
  const [localEdges, setLocalEdges, onEdgesChange] = useEdgesState(edges)

  React.useEffect(() => {
    setLocalNodes(nodes)
  }, [nodes, setLocalNodes])

  React.useEffect(() => {
    setLocalEdges(edges)
  }, [edges, setLocalEdges])

  React.useEffect(() => {
    setNodes(localNodes)
  }, [localNodes, setNodes])

  React.useEffect(() => {
    setEdges(localEdges)
  }, [localEdges, setEdges])

  const onConnect = useCallback(
    (params) => {
      setLocalEdges((eds) => addEdge({
        ...params,
        animated: true,
        style: { stroke: '#3d3d5c', strokeWidth: 2 }
      }, eds))
    },
    [setLocalEdges]
  )

  const onDragOver = useCallback((event) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event) => {
      event.preventDefault()

      const type = event.dataTransfer.getData('application/reactflow')
      if (!type) return

      const reactFlowBounds = reactFlowWrapper.current.getBoundingClientRect()
      const position = {
        x: event.clientX - reactFlowBounds.left - 90,
        y: event.clientY - reactFlowBounds.top - 30
      }

      addNode(type, position)
    },
    [addNode]
  )

  const onNodeClick = useCallback(
    (event, node) => {
      selectNode(node)
    },
    [selectNode]
  )

  const onPaneClick = useCallback(() => {
    selectNode(null)
  }, [selectNode])

  if (!currentWorkflow) {
    return (
      <div className="canvas-container">
        <div className="empty-state">
          <h3>No Workflow Selected</h3>
          <p>Create a new workflow or select an existing one from the sidebar</p>
        </div>
      </div>
    )
  }

  return (
    <div className="canvas-container" ref={reactFlowWrapper}>
      <ReactFlow
        nodes={localNodes}
        edges={localEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onDrop={onDrop}
        onDragOver={onDragOver}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={nodeTypes}
        fitView
        snapToGrid
        snapGrid={[15, 15]}
      >
        <Background color="#2d2d44" gap={20} />
        <Controls />
        <MiniMap
          nodeColor={(node) => {
            const colors = {
              api: '#3b82f6',
              sql: '#10b981',
              python: '#f59e0b',
              decision: '#8b5cf6',
              wait: '#6b7280',
              llm: '#ec4899',
              airflow: '#06b6d4'
            }
            return colors[node.type] || '#666'
          }}
        />
      </ReactFlow>
    </div>
  )
}

export default function Canvas() {
  return (
    <ReactFlowProvider>
      <FlowCanvas />
    </ReactFlowProvider>
  )
}
