import { state } from './state.js'

export function generateBlastRadiusAnalysis(nodeName) {
    if (!state.currentConfig || !nodeName) return null

    const pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []

    // Check if this is a pipeline group
    const groupMembers = pipelines.filter((p) => p.group === nodeName)
    const isGroup = groupMembers.length > 0

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

    // For groups, start BFS from all member pipelines
    // For regular nodes, start from the node itself
    const startNodes = isGroup ? groupMembers.map((p) => p.name) : [nodeName]
    const groupMemberNames = new Set(startNodes)

    startNodes.forEach((startNode) => {
        if (!visited.has(startNode)) {
            visited.set(startNode, 0)
        }
    })

    // Use all start nodes as initial queue
    const queue = startNodes.map((n) => [n, 0])

    while (queue.length > 0) {
        const [current, depth] = queue.shift()
        const neighbors = downstream.get(current) || new Set()

        for (const neighbor of neighbors) {
            // For groups, don't add edges between group members
            if (isGroup && groupMemberNames.has(current) && groupMemberNames.has(neighbor)) {
                continue
            }
            edges.push({ source: isGroup ? nodeName : current, target: neighbor })
            if (!visited.has(neighbor)) {
                visited.set(neighbor, depth + 1)
                queue.push([neighbor, depth + 1])
            }
        }
    }

    const downstreamNodes = []
    visited.forEach((depth, node) => {
        // Skip the source node itself, and for groups, skip all group members
        const isSourceOrMember = isGroup ? groupMemberNames.has(node) : node === nodeName
        if (!isSourceOrMember) {
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

    const result = {
        source: nodeName,
        source_type: isGroup ? 'group' : nodeTypes.get(nodeName) || 'unknown',
        total_affected: downstreamNodes.length,
        max_depth: maxDepth,
        downstream: downstreamNodes,
        by_depth: byDepth,
        edges
    }

    // Add group member info if this is a group
    if (isGroup) {
        result.group_members = groupMembers.map((p) => p.name)
        result.group_size = groupMembers.length
    }

    return result
}

export function generateBlastRadiusDot(analysis) {
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
    const isGroup = analysis.source_type === 'group'
    const sourceShape = analysis.source_type === 'datasource' ? 'ellipse' : 'box'
    const sourceLabel = isGroup ? `${analysis.source}\\n(${analysis.group_size} pipelines)` : analysis.source
    const sourceBorderColor = isGroup ? '#00897b' : sourceColor.border
    dot += `    "${analysis.source}" [label="${sourceLabel}" shape="${sourceShape}" fillcolor="${sourceColor.fill}" color="${sourceBorderColor}" fontcolor="${sourceColor.text}" penwidth="2"]\n\n`

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
