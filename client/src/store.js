import { create } from 'zustand'

const API_BASE = '/api'

export const useWorkflowStore = create((set, get) => ({
  workflows: [],
  currentWorkflow: null,
  nodes: [],
  edges: [],
  selectedNode: null,
  executionStatus: null,
  executionLogs: [],
  nodeResults: {},

  fetchWorkflows: async () => {
    try {
      const res = await fetch(`${API_BASE}/workflows`)
      const data = await res.json()
      set({ workflows: data })
    } catch (error) {
      console.error('Failed to fetch workflows:', error)
    }
  },

  createWorkflow: async (name = 'New Workflow') => {
    try {
      const res = await fetch(`${API_BASE}/workflows`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, nodes: [], edges: [] })
      })
      const data = await res.json()
      set(state => ({
        workflows: [data, ...state.workflows],
        currentWorkflow: data,
        nodes: [],
        edges: []
      }))
      return data
    } catch (error) {
      console.error('Failed to create workflow:', error)
    }
  },

  loadWorkflow: async (workflowId) => {
    try {
      const res = await fetch(`${API_BASE}/workflows/${workflowId}`)
      const data = await res.json()
      set({
        currentWorkflow: data,
        nodes: data.nodes || [],
        edges: data.edges || [],
        selectedNode: null
      })
    } catch (error) {
      console.error('Failed to load workflow:', error)
    }
  },

  saveWorkflow: async () => {
    const { currentWorkflow, nodes, edges } = get()
    if (!currentWorkflow) return

    try {
      const res = await fetch(`${API_BASE}/workflows/${currentWorkflow.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: currentWorkflow.name,
          description: currentWorkflow.description,
          nodes,
          edges
        })
      })
      const data = await res.json()
      set({ currentWorkflow: data })
    } catch (error) {
      console.error('Failed to save workflow:', error)
    }
  },

  deleteWorkflow: async (workflowId) => {
    try {
      await fetch(`${API_BASE}/workflows/${workflowId}`, { method: 'DELETE' })
      set(state => ({
        workflows: state.workflows.filter(w => w.id !== workflowId),
        currentWorkflow: state.currentWorkflow?.id === workflowId ? null : state.currentWorkflow,
        nodes: state.currentWorkflow?.id === workflowId ? [] : state.nodes,
        edges: state.currentWorkflow?.id === workflowId ? [] : state.edges
      }))
    } catch (error) {
      console.error('Failed to delete workflow:', error)
    }
  },

  updateWorkflowName: (name) => {
    set(state => ({
      currentWorkflow: state.currentWorkflow ? { ...state.currentWorkflow, name } : null
    }))
  },

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),

  addNode: (nodeType, position) => {
    const nodeDefaults = {
      api: { label: 'API Call', url: '', method: 'GET', headers: {}, body: '' },
      sql: { label: 'SQL Query', query: '', database: 'workflow.db' },
      python: { label: 'Python Code', code: 'result = {"value": "Hello"}' },
      decision: { label: 'Decision', condition: 'True' },
      wait: { label: 'Wait', seconds: 10 },
      llm: { label: 'LLM Generate', prompt: '', code_type: 'python', execute_code: false },
      airflow: { label: 'Airflow', action: 'trigger', base_url: '', dag_id: '', run_id: '', auth: {} }
    }

    const newNode = {
      id: `${nodeType}_${Date.now()}`,
      type: nodeType,
      position,
      data: { ...nodeDefaults[nodeType] }
    }

    set(state => ({ nodes: [...state.nodes, newNode] }))
    return newNode
  },

  updateNode: (nodeId, data) => {
    set(state => {
      const updatedNodes = state.nodes.map(node =>
        node.id === nodeId ? { ...node, data: { ...node.data, ...data } } : node
      )
      const updatedSelectedNode = state.selectedNode?.id === nodeId
        ? { ...state.selectedNode, data: { ...state.selectedNode.data, ...data } }
        : state.selectedNode
      return { nodes: updatedNodes, selectedNode: updatedSelectedNode }
    })
  },

  deleteNode: (nodeId) => {
    set(state => ({
      nodes: state.nodes.filter(node => node.id !== nodeId),
      edges: state.edges.filter(edge => edge.source !== nodeId && edge.target !== nodeId),
      selectedNode: state.selectedNode?.id === nodeId ? null : state.selectedNode
    }))
  },

  selectNode: (node) => set({ selectedNode: node }),

  executeWorkflow: async () => {
    const { currentWorkflow } = get()
    if (!currentWorkflow) return

    set({ executionStatus: 'running', executionLogs: [], nodeResults: {} })

    try {
      const res = await fetch(`${API_BASE}/workflows/${currentWorkflow.id}/execute`, {
        method: 'POST'
      })
      const data = await res.json()

      if (data.error) {
        set({
          executionStatus: 'failed',
          executionLogs: [{ type: 'error', message: data.error }],
          nodeResults: {}
        })
      } else {
        const logs = Object.entries(data.node_results || {}).map(([nodeId, result]) => ({
          nodeId,
          type: result.status,
          message: result.logs || result.error || JSON.stringify(result.outputs)
        }))
        set({
          executionStatus: data.status === 'failed' ? 'failed' : 'success',
          executionLogs: logs,
          nodeResults: data.node_results || {}
        })
      }
    } catch (error) {
      set({
        executionStatus: 'failed',
        executionLogs: [{ type: 'error', message: error.message }]
      })
    }
  },

  exportToPython: async () => {
    const { currentWorkflow } = get()
    if (!currentWorkflow) return null

    try {
      const res = await fetch(`${API_BASE}/workflows/${currentWorkflow.id}/export/python`)
      return await res.text()
    } catch (error) {
      console.error('Failed to export:', error)
      return null
    }
  },

  generateWithLLM: async (prompt, codeType, context = {}) => {
    try {
      const res = await fetch(`${API_BASE}/llm/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt, code_type: codeType, context })
      })
      return await res.json()
    } catch (error) {
      console.error('Failed to generate:', error)
      return { error: error.message }
    }
  }
}))
