import { state } from './state.js'
import { generateGraphvizDot } from './graph.js'

const WAVE_COLORS = [
    { fill: '#e8f5e9', border: '#81c784', text: '#495057' },
    { fill: '#e3f2fd', border: '#64b5f6', text: '#495057' },
    { fill: '#f3e5f5', border: '#ba68c8', text: '#495057' },
    { fill: '#fff3e0', border: '#ffb74d', text: '#495057' },
    { fill: '#e0f7fa', border: '#4dd0e1', text: '#495057' },
    { fill: '#fce4ec', border: '#f48fb1', text: '#495057' }
]

const WAVE_COLORS_DARK = [
    { fill: '#2a4a3a', border: '#81c784', text: '#e0e0e0' },
    { fill: '#2a3a4a', border: '#64b5f6', text: '#e0e0e0' },
    { fill: '#3a2a4a', border: '#ba68c8', text: '#e0e0e0' },
    { fill: '#4a3a2a', border: '#ffb74d', text: '#e0e0e0' },
    { fill: '#2a4a4a', border: '#4dd0e1', text: '#e0e0e0' },
    { fill: '#4a2a3a', border: '#f48fb1', text: '#e0e0e0' }
]

const WAVE0_COLOR = { fill: '#fef3e2', border: '#d4915c', text: '#495057' }
const WAVE0_COLOR_DARK = { fill: '#4a3a2a', border: '#d4915c', text: '#e0e0e0' }

let currentExportFormat = 'json'
let currentBackfillView = 'pipeline'
let backfillGraphInstance = null
let selectedBackfillPipelines = []

export function generateGraphExport() {
    if (!state.currentConfig) return null

    const nodes = []
    const edges = []
    const nodeIds = new Set()
    const pipelines = state.currentConfig.pipelines || []
    pipelines.forEach((p) => {
        const node = {
            id: p.name,
            type: 'pipeline'
        }
        if (p.description) node.description = p.description
        if (p.schedule) node.schedule = p.schedule
        if (p.cluster) node.cluster = p.cluster
        if (p.group) node.group = p.group
        if (p.owner) node.owner = p.owner
        if (p.tags?.length) node.tags = p.tags
        if (p.links && Object.keys(p.links).length) node.links = p.links
        if (p.metadata && Object.keys(p.metadata).length) node.metadata = p.metadata

        nodes.push(node)
        nodeIds.add(p.name)

        p.input_sources?.forEach((source) => {
            edges.push({ source, target: p.name, type: 'data_flow' })
        })
        p.output_sources?.forEach((source) => {
            edges.push({ source: p.name, target: source, type: 'data_flow' })
        })
        if (p.upstream_pipelines) {
            p.upstream_pipelines.forEach((upstream) => {
                edges.push({ source: upstream, target: p.name, type: 'pipeline_dependency' })
            })
        }
    })

    const datasources = state.currentConfig.datasources || []
    datasources.forEach((ds) => {
        const node = {
            id: ds.name,
            type: 'datasource'
        }
        if (ds.description) node.description = ds.description
        if (ds.type) node.source_type = ds.type
        if (ds.cluster) node.cluster = ds.cluster
        if (ds.owner) node.owner = ds.owner
        if (ds.tags?.length) node.tags = ds.tags
        if (ds.links && Object.keys(ds.links).length) node.links = ds.links
        if (ds.metadata && Object.keys(ds.metadata).length) node.metadata = ds.metadata

        nodes.push(node)
        nodeIds.add(ds.name)
    })

    // Add implicit datasources (referenced but not defined)
    const referencedSources = new Set()
    pipelines.forEach((p) => {
        p.input_sources?.forEach((s) => referencedSources.add(s))
        p.output_sources?.forEach((s) => referencedSources.add(s))
    })
    referencedSources.forEach((source) => {
        if (!nodeIds.has(source)) {
            nodes.push({
                id: source,
                type: 'datasource',
                implicit: true
            })
            nodeIds.add(source)
        }
    })

    const clusters = state.currentConfig.clusters || []

    return {
        nodes,
        edges,
        clusters: clusters.map((c) => ({
            id: c.name,
            description: c.description,
            parent: c.parent
        })),
        meta: {
            node_count: nodes.length,
            edge_count: edges.length,
            pipeline_count: pipelines.length,
            datasource_count: nodes.filter((n) => n.type === 'datasource').length,
            generated_at: new Date().toISOString()
        }
    }
}

export function generateMermaidExport() {
    if (!state.currentConfig) return null

    const pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []

    const nodeIds = new Set()
    const pipelineIds = new Set()
    pipelines.forEach((p) => {
        nodeIds.add(p.name)
        pipelineIds.add(p.name)
    })
    datasources.forEach((ds) => nodeIds.add(ds.name))

    pipelines.forEach((p) => {
        p.input_sources?.forEach((s) => nodeIds.add(s))
        p.output_sources?.forEach((s) => nodeIds.add(s))
    })

    const sanitize = (id) => id.replace(/[^a-zA-Z0-9]/g, '_')

    let mermaid = 'flowchart LR\n'
    mermaid += '    %% Nodes\n'
    nodeIds.forEach((id) => {
        const sId = sanitize(id)
        if (pipelineIds.has(id)) {
            mermaid += `    ${sId}[["${id}"]]\n` // Stadium shape for pipelines
        } else {
            mermaid += `    ${sId}[("${id}")]\n` // Cylinder for datasources
        }
    })

    mermaid += '\n    %% Edges\n'
    const edges = new Set()
    pipelines.forEach((p) => {
        const pId = sanitize(p.name)
        p.input_sources?.forEach((source) => {
            const edge = `    ${sanitize(source)} --> ${pId}`
            edges.add(edge)
        })
        p.output_sources?.forEach((source) => {
            const edge = `    ${pId} --> ${sanitize(source)}`
            edges.add(edge)
        })
        p.upstream_pipelines?.forEach((upstream) => {
            const edge = `    ${sanitize(upstream)} -.-> ${pId}`
            edges.add(edge)
        })
    })

    edges.forEach((edge) => {
        mermaid += edge + '\n'
    })

    mermaid += '\n    %% Styling\n'
    mermaid += '    classDef pipeline fill:#fff3e0,stroke:#e65100,stroke-width:2px\n'
    mermaid += '    classDef datasource fill:#e3f2fd,stroke:#1565c0,stroke-width:2px\n'
    const pipelineList = [...pipelineIds].map(sanitize).join(',')
    const datasourceList = [...nodeIds]
        .filter((id) => !pipelineIds.has(id))
        .map(sanitize)
        .join(',')

    if (pipelineList) mermaid += `    class ${pipelineList} pipeline\n`
    if (datasourceList) mermaid += `    class ${datasourceList} datasource\n`

    return mermaid
}

export function generateBackfillAnalysis(nodeNames) {
    if (!state.currentConfig) return null

    const sourceNodes = Array.isArray(nodeNames) ? nodeNames : [nodeNames]
    if (sourceNodes.length === 0) return null

    const pipelines = state.currentConfig.pipelines || []
    const pipelineNames = new Set(pipelines.map((p) => p.name))

    const invalidNodes = sourceNodes.filter((n) => !pipelineNames.has(n))
    if (invalidNodes.length > 0) {
        return {
            nodes: sourceNodes,
            message: `Backfill planning is only available for pipelines. Invalid: ${invalidNodes.join(', ')}`,
            waves: []
        }
    }

    const downstreamMap = {}

    pipelines.forEach((p) => {
        if (p.upstream_pipelines) {
            p.upstream_pipelines.forEach((upstream) => {
                if (!downstreamMap[upstream]) downstreamMap[upstream] = []
                downstreamMap[upstream].push(p.name)
            })
        }
    })

    const outputToProducer = {}
    pipelines.forEach((p) => {
        if (p.output_sources) {
            p.output_sources.forEach((ds) => {
                outputToProducer[ds] = p.name
            })
        }
    })

    pipelines.forEach((p) => {
        if (p.input_sources) {
            p.input_sources.forEach((ds) => {
                const producer = outputToProducer[ds]
                if (producer && producer !== p.name) {
                    if (!downstreamMap[producer]) downstreamMap[producer] = []
                    if (!downstreamMap[producer].includes(p.name)) {
                        downstreamMap[producer].push(p.name)
                    }
                }
            })
        }
    })

    function getReachable(startNode) {
        const reachable = new Set()
        const q = [startNode]
        while (q.length > 0) {
            const curr = q.shift()
            const children = downstreamMap[curr] || []
            for (const child of children) {
                if (!reachable.has(child)) {
                    reachable.add(child)
                    q.push(child)
                }
            }
        }
        return reachable
    }

    const sourceSet = new Set(sourceNodes)
    const reachableFromSources = new Map()

    sourceNodes.forEach((node) => {
        reachableFromSources.set(node, getReachable(node))
    })

    const trueSources = sourceNodes.filter((node) => {
        for (const otherNode of sourceNodes) {
            if (otherNode !== node && reachableFromSources.get(otherNode).has(node)) {
                return false // this node is reachable from another selected node
            }
        }
        return true
    })

    const downstreamSelectedNodes = sourceNodes.filter((node) => !trueSources.includes(node))
    const visited = new Set(trueSources)
    const allDownstream = []
    const queue = trueSources.map((n) => ({ name: n, depth: 0 }))

    while (queue.length > 0) {
        const current = queue.shift()
        const children = downstreamMap[current.name] || []

        children.forEach((child) => {
            if (!visited.has(child)) {
                visited.add(child)
                allDownstream.push({ name: child, depth: current.depth + 1 })
                queue.push({ name: child, depth: current.depth + 1 })
            }
        })
    }

    downstreamSelectedNodes.forEach((node) => {
        if (!allDownstream.find((d) => d.name === node)) {
            allDownstream.push({ name: node, depth: 1 }) // depth doesn't matter, topological sort will place it
        }
    })

    if (allDownstream.length === 0 && trueSources.length === sourceNodes.length) {
        return {
            nodes: sourceNodes,
            message: 'No downstream pipelines to backfill.',
            waves: []
        }
    }

    const downstreamNames = new Set(allDownstream.map((d) => d.name))
    const edges = []

    trueSources.forEach((sourceNode) => {
        const directDownstream = downstreamMap[sourceNode] || []
        directDownstream.forEach((target) => {
            if (downstreamNames.has(target)) {
                edges.push({ source: sourceNode, target })
            }
        })
    })

    downstreamNames.forEach((node) => {
        const children = downstreamMap[node] || []
        children.forEach((target) => {
            if (downstreamNames.has(target)) {
                edges.push({ source: node, target })
            }
        })
    })

    const inDegree = {}
    const graph = {}
    downstreamNames.forEach((node) => {
        inDegree[node] = 0
        graph[node] = []
    })

    edges.forEach(({ source, target }) => {
        if (downstreamNames.has(source) && downstreamNames.has(target)) {
            graph[source].push(target)
            inDegree[target]++
        }
    })

    let waveQueue = Object.keys(inDegree).filter((n) => inDegree[n] === 0)
    const waves = []

    while (waveQueue.length > 0) {
        const waveNodes = waveQueue.map((name) => {
            const pipeline = pipelines.find((p) => p.name === name)
            const nodeInfo = { name }
            if (pipeline) {
                if (pipeline.schedule) nodeInfo.schedule = pipeline.schedule
                if (pipeline.owner) nodeInfo.owner = pipeline.owner
                if (pipeline.cluster) nodeInfo.cluster = pipeline.cluster
            }
            return nodeInfo
        })

        waves.push(waveNodes)

        const nextQueue = []
        waveQueue.forEach((node) => {
            graph[node].forEach((neighbor) => {
                inDegree[neighbor]--
                if (inDegree[neighbor] === 0) {
                    nextQueue.push(neighbor)
                }
            })
        })
        waveQueue = nextQueue
    }

    const wave0Pipelines = trueSources.map((nodeName) => {
        const pipeline = pipelines.find((p) => p.name === nodeName)
        const nodeInfo = { name: nodeName }
        if (pipeline) {
            if (pipeline.schedule) nodeInfo.schedule = pipeline.schedule
            if (pipeline.owner) nodeInfo.owner = pipeline.owner
            if (pipeline.cluster) nodeInfo.cluster = pipeline.cluster
        }
        return nodeInfo
    })

    const allWaves = [
        { wave: 0, parallel_count: wave0Pipelines.length, pipelines: wave0Pipelines },
        ...waves.map((nodes, idx) => ({
            wave: idx + 1,
            parallel_count: nodes.length,
            pipelines: nodes
        }))
    ]

    return {
        nodes: sourceNodes,
        total_downstream_pipelines: allDownstream.length,
        total_waves: allWaves.length,
        max_parallelism: Math.max(...allWaves.map((w) => w.parallel_count), 1),
        waves: allWaves,
        edges
    }
}

export function setExportFormat(format) {
    currentExportFormat = format
    updateExportView()

    document.querySelectorAll('.export-format-btn').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.format === format)
    })
}

export function updateExportView() {
    const output = document.getElementById('export-output')
    if (!output) return

    let content = ''

    if (currentExportFormat === 'mermaid') {
        content = generateMermaidExport() || 'Load a configuration to see the Mermaid export...'
    } else if (currentExportFormat === 'dot') {
        content = generateGraphvizDot() || 'Load a configuration to see the DOT export...'
    } else {
        const graphExport = generateGraphExport()
        content = graphExport ? JSON.stringify(graphExport, null, 2) : 'Load a configuration to see the graph export...'
    }

    output.textContent = content
}

export function copyExportToClipboard(event) {
    const output = document.getElementById('export-output')
    if (!output) return

    const btn = event.target.closest('.export-copy-btn')

    navigator.clipboard.writeText(output.textContent).then(() => {
        if (btn) {
            btn.classList.add('copied')
            btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"></polyline></svg>`
            setTimeout(() => {
                btn.classList.remove('copied')
                btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>`
            }, 1500)
        }
    })
}

export function populateBackfillSelect() {
    const list = document.getElementById('backfill-picker-list')
    if (!list) return

    list.innerHTML = ''
    selectedBackfillPipelines = []
    updatePickerButton()
    updatePickerCount()

    const filterInput = document.getElementById('backfill-picker-filter')
    if (filterInput) filterInput.value = ''

    if (filterInput) {
        filterInput.placeholder = currentBackfillView === 'blast' ? 'Filter nodes...' : 'Filter pipelines...'
    }

    if (!state.currentConfig) return

    const pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []
    const isBlastMode = currentBackfillView === 'blast'

    const grouped = {}
    pipelines.forEach((p) => {
        const group = p.cluster || p.group || 'Pipelines'
        if (!grouped[group]) grouped[group] = []
        grouped[group].push({ name: p.name, type: 'pipeline' })
    })

    if (isBlastMode) {
        datasources.forEach((ds) => {
            const group = ds.cluster || 'Datasources'
            if (!grouped[group]) grouped[group] = []
            grouped[group].push({ name: ds.name, type: 'datasource' })
        })
    }

    const sortedGroups = Object.keys(grouped).sort()

    sortedGroups.forEach((groupName) => {
        const groupLabel = document.createElement('div')
        groupLabel.className = 'backfill-picker-group'
        groupLabel.textContent = groupName
        groupLabel.dataset.group = groupName
        list.appendChild(groupLabel)

        grouped[groupName].forEach((item) => {
            const itemEl = document.createElement('label')
            itemEl.className = 'backfill-picker-item'
            itemEl.dataset.pipeline = item.name.toLowerCase()
            itemEl.dataset.group = groupName

            const input = document.createElement('input')
            input.type = isBlastMode ? 'radio' : 'checkbox'
            input.name = isBlastMode ? 'blast-node' : ''
            input.value = item.name
            input.addEventListener('change', handlePipelineCheckChange)

            itemEl.appendChild(input)
            itemEl.appendChild(document.createTextNode(item.name))

            if (item.type === 'datasource') {
                const tag = document.createElement('span')
                tag.className = 'picker-ds-tag'
                tag.textContent = 'ds'
                itemEl.appendChild(tag)
            }

            list.appendChild(itemEl)
        })
    })
}

function handlePipelineCheckChange(e) {
    const value = e.target.value
    const isBlastMode = currentBackfillView === 'blast'

    if (isBlastMode) {
        selectedBackfillPipelines = [value]
    } else {
        if (e.target.checked) {
            if (!selectedBackfillPipelines.includes(value)) {
                selectedBackfillPipelines.push(value)
            }
        } else {
            selectedBackfillPipelines = selectedBackfillPipelines.filter((p) => p !== value)
        }
    }
    updatePickerButton()
    updatePickerCount()
    updateBackfillPlan()
}

export function filterBackfillPipelines(query) {
    const list = document.getElementById('backfill-picker-list')
    if (!list) return

    const lowerQuery = query.toLowerCase().trim()
    const visibleGroups = new Set()

    list.querySelectorAll('.backfill-picker-item').forEach((item) => {
        const pipelineName = item.dataset.pipeline || ''
        const matches = !lowerQuery || pipelineName.includes(lowerQuery)
        item.classList.toggle('hidden', !matches)
        if (matches) {
            visibleGroups.add(item.dataset.group)
        }
    })

    list.querySelectorAll('.backfill-picker-group').forEach((label) => {
        label.classList.toggle('hidden', !visibleGroups.has(label.dataset.group))
    })
}

export function toggleBackfillPicker() {
    const dropdown = document.getElementById('backfill-picker-dropdown')
    if (dropdown) {
        const isOpen = dropdown.classList.toggle('show')
        if (isOpen) {
            const filterInput = document.getElementById('backfill-picker-filter')
            if (filterInput) {
                setTimeout(() => filterInput.focus(), 0)
            }
            setTimeout(() => {
                document.addEventListener('click', closePickerOnClickOutside)
            }, 0)
        }
    }
}

function closePickerOnClickOutside(e) {
    const picker = document.querySelector('.backfill-pipeline-picker')
    if (picker && !picker.contains(e.target)) {
        const dropdown = document.getElementById('backfill-picker-dropdown')
        if (dropdown) dropdown.classList.remove('show')
        document.removeEventListener('click', closePickerOnClickOutside)
    }
}

export function clearBackfillSelection() {
    const list = document.getElementById('backfill-picker-list')
    if (list) {
        list.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
            cb.checked = false
        })
    }
    selectedBackfillPipelines = []
    updatePickerButton()
    updatePickerCount()
    updateBackfillPlan()
}

function updatePickerButton() {
    const btn = document.getElementById('backfill-picker-btn')
    if (!btn) return

    const isBlast = currentBackfillView === 'blast'

    if (selectedBackfillPipelines.length === 0) {
        btn.textContent = isBlast ? 'Select a node...' : 'Select pipelines...'
    } else if (selectedBackfillPipelines.length === 1) {
        btn.textContent = selectedBackfillPipelines[0]
    } else {
        btn.textContent = isBlast
            ? selectedBackfillPipelines[0]
            : `${selectedBackfillPipelines.length} pipelines selected`
    }
}

function updatePickerCount() {
    const countEl = document.getElementById('backfill-picker-count')
    if (countEl) {
        const count = selectedBackfillPipelines.length
        countEl.textContent = count === 0 ? 'None selected' : count === 1 ? '1 selected' : `${count} selected`
    }
}

function generateBackfillDot(analysis) {
    if (!analysis || !analysis.waves || analysis.waves.length === 0) return null

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const bgColor = isDark ? '#1a1a1a' : '#ffffff'
    const textColor = isDark ? '#b0b0b0' : '#666666'
    const edgeColor = isDark ? '#666666' : '#999999'
    const waveColors = isDark ? WAVE_COLORS_DARK : WAVE_COLORS
    const wave0Color = isDark ? WAVE0_COLOR_DARK : WAVE0_COLOR

    let dot = `digraph BackfillPlan {
    rankdir=LR
    bgcolor="${bgColor}"
    fontname="Helvetica"
    node [fontname="Helvetica" fontsize="9" style="filled,rounded" shape="box"]
    edge [color="${edgeColor}" arrowsize="0.6"]
    
`

    analysis.waves.forEach((wave) => {
        const colors = wave.wave === 0 ? wave0Color : waveColors[(wave.wave - 1) % waveColors.length]
        const pipelines = wave.pipelines || wave.nodes || []
        dot += `    subgraph cluster_wave${wave.wave} {
        label="Wave ${wave.wave}"
        fontname="Helvetica"
        style="dashed"
        color="${colors.border}"
        fontcolor="${textColor}"
        fontsize="9"
        
`
        pipelines.forEach((node) => {
            dot += `        "${node.name}" [label="${node.name}" fillcolor="${colors.fill}" color="${colors.border}" fontcolor="${colors.text}"]\n`
        })
        dot += `    }\n\n`
    })

    analysis.edges.forEach((edge) => {
        dot += `    "${edge.source}" -> "${edge.target}"\n`
    })

    dot += `}\n`
    return dot
}

const VIEW_HINTS = {
    pipeline: 'Select pipelines to plan backfill execution order',
    airflow: 'Maps pipelines to Airflow DAGs for backfill commands',
    blast: 'Select a node to see all downstream dependencies affected by changes'
}

export function setBackfillView(view) {
    currentBackfillView = view

    document.querySelectorAll('.backfill-view-btn').forEach((btn) => {
        btn.classList.toggle('active', btn.dataset.view === view)
    })

    const hint = document.getElementById('backfill-hint')
    if (hint) hint.textContent = VIEW_HINTS[view] || ''

    backfillGraphInstance = null
    const graphContainer = document.getElementById('backfill-graph')
    if (graphContainer && graphContainer.parentNode) {
        const parent = graphContainer.parentNode
        const newContainer = document.createElement('div')
        newContainer.id = 'backfill-graph'
        newContainer.className = 'backfill-graph'
        parent.replaceChild(newContainer, graphContainer)
    }

    populateBackfillSelect()
    updatePickerButton()
    updateBackfillPlan()
}

function extractAirflowDag(airflowUrl) {
    if (!airflowUrl) return null
    const match = airflowUrl.match(/\/dags\/([^\/\?#]+)/)
    return match ? match[1] : airflowUrl
}

function generateAirflowAnalysis(pipelineAnalysis) {
    if (!pipelineAnalysis || !pipelineAnalysis.waves) return null

    const pipelines = state.currentConfig?.pipelines || []
    const missingAirflow = []
    const pipelineToDag = {}
    pipelineAnalysis.waves.forEach((wave) => {
        wave.pipelines.forEach((p) => {
            const pipeline = pipelines.find((pl) => pl.name === p.name)
            const airflowUrl = pipeline?.links?.airflow

            if (!airflowUrl) {
                missingAirflow.push(p.name)
            } else {
                pipelineToDag[p.name] = {
                    dag: extractAirflowDag(airflowUrl),
                    airflow_url: airflowUrl
                }
            }
        })
    })

    if (missingAirflow.length > 0) {
        return {
            error: 'Missing Airflow links',
            message: `Cannot generate Airflow backfill plan. The following pipelines are missing airflow links: ${missingAirflow.join(', ')}`,
            missing_pipelines: missingAirflow
        }
    }

    const airflowWaves = pipelineAnalysis.waves.map((wave) => {
        const dagMap = new Map() // dag name -> { dag, airflow_url, pipelines: [] }

        wave.pipelines.forEach((p) => {
            const dagInfo = pipelineToDag[p.name]
            if (dagInfo) {
                if (!dagMap.has(dagInfo.dag)) {
                    dagMap.set(dagInfo.dag, {
                        dag: dagInfo.dag,
                        airflow_url: dagInfo.airflow_url,
                        pipelines: []
                    })
                }
                dagMap.get(dagInfo.dag).pipelines.push(p.name)
            }
        })

        return {
            wave: wave.wave,
            parallel_count: dagMap.size,
            dags: [...dagMap.values()]
        }
    })

    const edgeSet = new Set()
    const dedupedEdges = []

    pipelineAnalysis.edges.forEach((e) => {
        const sourceDag = pipelineToDag[e.source]?.dag || e.source
        const targetDag = pipelineToDag[e.target]?.dag || e.target
        const edgeKey = `${sourceDag}|${targetDag}`

        if (!edgeSet.has(edgeKey) && sourceDag !== targetDag) {
            edgeSet.add(edgeKey)
            dedupedEdges.push({
                source_dag: sourceDag,
                target_dag: targetDag
            })
        }
    })

    return {
        node: pipelineAnalysis.node,
        node_dag: pipelineToDag[pipelineAnalysis.node]?.dag || pipelineAnalysis.node,
        view: 'airflow',
        total_dags: new Set(Object.values(pipelineToDag).map((d) => d.dag)).size,
        total_waves: airflowWaves.length,
        waves: airflowWaves,
        edges: dedupedEdges
    }
}

function generateAirflowBackfillDot(analysis) {
    if (!analysis || !analysis.waves || analysis.waves.length === 0 || analysis.error) return null

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const bgColor = isDark ? '#1a1a1a' : '#ffffff'
    const textColor = isDark ? '#b0b0b0' : '#666666'
    const edgeColor = isDark ? '#666666' : '#999999'
    const wave0Color = isDark ? WAVE0_COLOR_DARK : WAVE0_COLOR
    const waveColors = isDark ? WAVE_COLORS_DARK : WAVE_COLORS

    let dot = `digraph AirflowBackfill {
    rankdir=LR
    bgcolor="${bgColor}"
    fontname="Helvetica"
    node [fontname="Helvetica" fontsize="9" style="filled,rounded" shape="box"]
    edge [color="${edgeColor}" arrowsize="0.6"]
    
`

    analysis.waves.forEach((wave) => {
        const colors = wave.wave === 0 ? wave0Color : waveColors[(wave.wave - 1) % waveColors.length]
        dot += `    subgraph cluster_wave${wave.wave} {
        label="Wave ${wave.wave}"
        fontname="Helvetica"
        style="dashed"
        color="${colors.border}"
        fontcolor="${textColor}"
        fontsize="9"
        
`
        wave.dags.forEach((d) => {
            dot += `        "${d.dag}" [label="${d.dag}" fillcolor="${colors.fill}" color="${colors.border}" fontcolor="${colors.text}"]\n`
        })
        dot += `    }\n\n`
    })

    analysis.edges.forEach((edge) => {
        dot += `    "${edge.source_dag}" -> "${edge.target_dag}"\n`
    })

    dot += `}\n`
    return dot
}

function generateBlastRadiusAnalysis(nodeName) {
    if (!state.currentConfig || !nodeName) return null

    const pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []

    const downstream = new Map()
    const nodeTypes = new Map()

    pipelines.forEach((p) => {
        nodeTypes.set(p.name, 'pipeline')
        if (!downstream.has(p.name)) downstream.set(p.name, new Set())

        p.output_sources?.forEach((s) => {
            downstream.get(p.name).add(s)
            if (!nodeTypes.has(s)) nodeTypes.set(s, 'datasource')
        })

        p.input_sources?.forEach((s) => {
            if (!downstream.has(s)) downstream.set(s, new Set())
            downstream.get(s).add(p.name)
            if (!nodeTypes.has(s)) nodeTypes.set(s, 'datasource')
        })

        p.upstream_pipelines?.forEach((u) => {
            if (!downstream.has(u)) downstream.set(u, new Set())
            downstream.get(u).add(p.name)
        })
    })

    datasources.forEach((ds) => {
        if (!nodeTypes.has(ds.name)) nodeTypes.set(ds.name, 'datasource')
    })

    const visited = new Map()
    const edges = []
    const queue = [[nodeName, 0]]
    visited.set(nodeName, 0)

    while (queue.length > 0) {
        const [current, depth] = queue.shift()
        const neighbors = downstream.get(current) || new Set()

        for (const neighbor of neighbors) {
            edges.push({ source: current, target: neighbor })
            if (!visited.has(neighbor)) {
                visited.set(neighbor, depth + 1)
                queue.push([neighbor, depth + 1])
            }
        }
    }

    const downstreamNodes = []
    visited.forEach((depth, node) => {
        if (node !== nodeName) {
            const type = nodeTypes.get(node) || 'unknown'
            const pipeline = pipelines.find((p) => p.name === node)
            const nodeInfo = { name: node, type, depth }
            if (pipeline) {
                if (pipeline.schedule) nodeInfo.schedule = pipeline.schedule
                if (pipeline.cluster) nodeInfo.cluster = pipeline.cluster
            }
            downstreamNodes.push(nodeInfo)
        }
    })

    downstreamNodes.sort((a, b) => a.depth - b.depth || a.name.localeCompare(b.name))
    const byDepth = {}
    downstreamNodes.forEach((n) => {
        if (!byDepth[n.depth]) byDepth[n.depth] = []
        byDepth[n.depth].push(n)
    })

    const maxDepth = Math.max(...downstreamNodes.map((n) => n.depth), 0)

    return {
        source: nodeName,
        source_type: nodeTypes.get(nodeName) || 'unknown',
        total_affected: downstreamNodes.length,
        max_depth: maxDepth,
        downstream: downstreamNodes,
        by_depth: byDepth,
        edges
    }
}

function generateBlastRadiusDot(analysis) {
    if (!analysis || analysis.downstream.length === 0) return null

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const bgColor = isDark ? '#1a1a1a' : '#ffffff'
    const textColor = isDark ? '#b0b0b0' : '#666666'
    const edgeColor = isDark ? '#666666' : '#999999'

    const depthColors = isDark
        ? [
              { fill: '#4a2a2a', border: '#c98b8b', text: '#e0e0e0' }, // Source - warm red
              { fill: '#4a3a2a', border: '#d4a574', text: '#e0e0e0' }, // Depth 1 - orange
              { fill: '#4a4a2a', border: '#c4c474', text: '#e0e0e0' }, // Depth 2 - yellow
              { fill: '#2a4a3a', border: '#7cb47c', text: '#e0e0e0' }, // Depth 3 - green
              { fill: '#2a3a4a', border: '#6b9dc4', text: '#e0e0e0' }, // Depth 4 - blue
              { fill: '#3a2a4a', border: '#a88bc4', text: '#e0e0e0' } // Depth 5+ - purple
          ]
        : [
              { fill: '#fce4ec', border: '#c98b8b', text: '#495057' }, // Source - warm red
              { fill: '#fff3e0', border: '#d4a574', text: '#495057' }, // Depth 1 - orange
              { fill: '#fffde7', border: '#c4c474', text: '#495057' }, // Depth 2 - yellow
              { fill: '#e8f5e9', border: '#7cb47c', text: '#495057' }, // Depth 3 - green
              { fill: '#e3f2fd', border: '#6b9dc4', text: '#495057' }, // Depth 4 - blue
              { fill: '#f3e5f5', border: '#a88bc4', text: '#495057' } // Depth 5+ - purple
          ]

    let dot = `digraph BlastRadius {
    rankdir=LR
    bgcolor="${bgColor}"
    fontname="Helvetica"
    node [fontname="Helvetica" fontsize="9" style="filled"]
    edge [color="${edgeColor}" arrowsize="0.6"]
    
`

    const sourceColor = depthColors[0]
    const sourceShape = analysis.source_type === 'datasource' ? 'ellipse' : 'box'
    dot += `    "${analysis.source}" [label="${analysis.source}" shape="${sourceShape}" fillcolor="${sourceColor.fill}" color="${sourceColor.border}" fontcolor="${sourceColor.text}" penwidth="2"]\n\n`

    Object.entries(analysis.by_depth).forEach(([depth, nodes]) => {
        const colorIdx = Math.min(parseInt(depth), depthColors.length - 1)
        const colors = depthColors[colorIdx]

        dot += `    subgraph cluster_depth${depth} {
        label="Depth ${depth}"
        fontname="Helvetica"
        style="dashed"
        color="${colors.border}"
        fontcolor="${textColor}"
        fontsize="9"
        
`
        nodes.forEach((node) => {
            const shape = node.type === 'datasource' ? 'ellipse' : 'box'
            dot += `        "${node.name}" [label="${node.name}" shape="${shape}" fillcolor="${colors.fill}" color="${colors.border}" fontcolor="${colors.text}"]\n`
        })
        dot += `    }\n\n`
    })

    const addedEdges = new Set()
    analysis.edges.forEach((edge) => {
        const key = `${edge.source}|${edge.target}`
        if (!addedEdges.has(key)) {
            addedEdges.add(key)
            dot += `    "${edge.source}" -> "${edge.target}"\n`
        }
    })

    dot += `}\n`
    return dot
}

export function updateBackfillPlan() {
    const output = document.getElementById('backfill-output')
    const graphContainer = document.getElementById('backfill-graph')
    if (!output) return

    if (selectedBackfillPipelines.length === 0) {
        output.textContent = 'Select pipelines to see the backfill plan...'
        if (graphContainer) {
            graphContainer.innerHTML =
                '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">Select pipelines to see the execution plan...</div>'
        }
        return
    }

    const pipelineAnalysis = generateBackfillAnalysis(selectedBackfillPipelines)

    if (!pipelineAnalysis) {
        output.textContent = 'Unable to generate backfill plan. Make sure a configuration is loaded.'
        if (graphContainer) {
            graphContainer.innerHTML =
                '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">Unable to generate plan</div>'
        }
        return
    }

    if (currentBackfillView === 'airflow') {
        const airflowAnalysis = generateAirflowAnalysis(pipelineAnalysis)

        if (airflowAnalysis.error) {
            output.textContent = JSON.stringify(airflowAnalysis, null, 2)
            if (graphContainer) {
                graphContainer.innerHTML = `<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">${airflowAnalysis.message}</div>`
            }
            return
        }

        output.textContent = JSON.stringify(airflowAnalysis, null, 2)

        if (graphContainer && airflowAnalysis.waves && airflowAnalysis.waves.length > 0) {
            const dot = generateAirflowBackfillDot(airflowAnalysis)
            if (dot) {
                try {
                    if (!backfillGraphInstance) {
                        graphContainer.innerHTML = ''
                        backfillGraphInstance = d3
                            .select('#backfill-graph')
                            .graphviz()
                            .width(graphContainer.clientWidth)
                            .height(450)
                            .fit(true)
                            .zoom(true)
                            .transition(function () {
                                return d3.transition().duration(300)
                            })
                    }
                    backfillGraphInstance.renderDot(dot)
                } catch (e) {
                    graphContainer.innerHTML =
                        '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">Error rendering graph</div>'
                }
            }
        }
        return
    }

    if (currentBackfillView === 'blast') {
        const sourceNode = selectedBackfillPipelines[0]
        const blastAnalysis = generateBlastRadiusAnalysis(sourceNode)

        if (!blastAnalysis || blastAnalysis.downstream.length === 0) {
            output.textContent = JSON.stringify(
                { node: sourceNode, downstream: [], message: 'No downstream dependencies' },
                null,
                2
            )
            if (graphContainer) {
                graphContainer.innerHTML =
                    '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">No downstream dependencies for this node</div>'
            }
            return
        }

        output.textContent = JSON.stringify(blastAnalysis, null, 2)

        if (graphContainer) {
            const dot = generateBlastRadiusDot(blastAnalysis)
            if (dot) {
                try {
                    if (!backfillGraphInstance) {
                        graphContainer.innerHTML = ''
                        backfillGraphInstance = d3
                            .select('#backfill-graph')
                            .graphviz()
                            .width(graphContainer.clientWidth)
                            .height(450)
                            .fit(true)
                            .zoom(true)
                            .transition(function () {
                                return d3.transition().duration(300)
                            })
                    }
                    backfillGraphInstance.renderDot(dot)
                } catch (e) {
                    graphContainer.innerHTML =
                        '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">Error rendering graph</div>'
                }
            }
        }
        return
    }

    output.textContent = JSON.stringify(pipelineAnalysis, null, 2)

    if (graphContainer && pipelineAnalysis.waves && pipelineAnalysis.waves.length > 0) {
        const dot = generateBackfillDot(pipelineAnalysis)
        if (dot) {
            try {
                if (!backfillGraphInstance) {
                    graphContainer.innerHTML = ''
                    backfillGraphInstance = d3
                        .select('#backfill-graph')
                        .graphviz()
                        .width(graphContainer.clientWidth)
                        .height(450)
                        .fit(true)
                        .zoom(true)
                        .transition(function () {
                            return d3.transition().duration(300)
                        })
                }
                backfillGraphInstance.renderDot(dot)
            } catch (e) {
                graphContainer.innerHTML =
                    '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">Error rendering graph</div>'
            }
        }
    } else if (graphContainer) {
        graphContainer.innerHTML =
            '<div style="color: var(--text-muted); font-size: 12px; padding: 2rem; text-align: center;">No downstream dependencies to visualize</div>'
    }
}

export function copyBackfillToClipboard(event) {
    const output = document.getElementById('backfill-output')
    if (!output) return

    const btn = event.target.closest('.export-copy-btn')

    navigator.clipboard.writeText(output.textContent).then(() => {
        if (btn) {
            btn.classList.add('copied')
            btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"></polyline></svg>`
            setTimeout(() => {
                btn.classList.remove('copied')
                btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>`
            }, 1500)
        }
    })
}
