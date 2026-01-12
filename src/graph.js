import { state, getConfigHash, getViewStateKey, clearViewStateCache, addToViewCache } from './state.js'
import { renderAttributeGraph } from './attributes.js'
import { generateBlastRadiusAnalysis, generateBlastRadiusDot } from './blastradius.js'

let blastRadiusGraphInstance = null

export function generateGraphvizDot() {
    if (!state.currentConfig) return ''

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const edgeColor = isDark ? '#b0b0b0' : '#555'

    let pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []
    const explicitClusters = state.currentConfig.clusters || []

    if (state.groupedView) {
        const groups = new Map()
        const ungroupedPipelines = []

        pipelines.forEach((p) => {
            if (p.group && !state.expandedGroups.has(p.group)) {
                if (!groups.has(p.group)) {
                    groups.set(p.group, {
                        name: p.group,
                        description: `Grouped pipelines: ${p.group}`,
                        input_sources: new Set(),
                        output_sources: new Set(),
                        upstream_pipelines: new Set(),
                        cluster: p.cluster,
                        _isGroup: true,
                        _members: []
                    })
                }
                const group = groups.get(p.group)
                p.input_sources?.forEach((s) => group.input_sources.add(s))
                p.output_sources?.forEach((s) => group.output_sources.add(s))
                p.upstream_pipelines?.forEach((u) => group.upstream_pipelines.add(u))
                group._members.push(p.name)
            } else {
                ungroupedPipelines.push(p)
            }
        })

        const groupedPipelines = Array.from(groups.values()).map((g) => ({
            ...g,
            input_sources: Array.from(g.input_sources),
            output_sources: Array.from(g.output_sources),
            upstream_pipelines: Array.from(g.upstream_pipelines).filter((u) => !g._members.includes(u))
        }))

        pipelines = [...ungroupedPipelines, ...groupedPipelines]
    }

    const allDataSources = new Map()
    datasources.forEach((ds) => allDataSources.set(ds.name, ds))

    pipelines.forEach((pipeline) => {
        pipeline.input_sources?.forEach((sourceName) => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, { name: sourceName, type: 'auto-created' })
            }
        })
        pipeline.output_sources?.forEach((sourceName) => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, { name: sourceName, type: 'auto-created' })
            }
        })
    })

    const allClusterNames = new Set()
    pipelines.forEach((p) => {
        if (p.cluster) allClusterNames.add(p.cluster)
    })
    allDataSources.forEach((ds) => {
        if (ds.cluster) allClusterNames.add(ds.cluster)
    })

    const clusterDefinitions = new Map()
    const clusterHierarchy = new Map() // child -> parent
    const clusterChildren = new Map() // parent -> [children]

    explicitClusters.forEach((cluster) => {
        clusterDefinitions.set(cluster.name, cluster)
        allClusterNames.add(cluster.name)

        if (cluster.parent) {
            clusterHierarchy.set(cluster.name, cluster.parent)
            if (!clusterChildren.has(cluster.parent)) {
                clusterChildren.set(cluster.parent, [])
            }
            clusterChildren.get(cluster.parent).push(cluster.name)
            allClusterNames.add(cluster.parent)
        }
    })

    allClusterNames.forEach((name) => {
        if (!clusterDefinitions.has(name)) {
            clusterDefinitions.set(name, { name, description: `Auto-generated cluster: ${name}` })
        }
    })

    let dot = `digraph PipevizGraph {
    rankdir=LR;
    bgcolor="transparent";
    node [fontsize=12];
    edge [fontsize=10];

`

    const clusterColors = [
        '#1976d2',
        '#7b1fa2',
        '#388e3c',
        '#f57c00',
        '#d32f2f',
        '#1976d2',
        '#616161',
        '#c2185b',
        '#303f9f',
        '#f57c00'
    ]

    const nodesByCluster = new Map()

    pipelines.forEach((pipeline) => {
        const cluster = pipeline.cluster || '_unclustered'
        if (!nodesByCluster.has(cluster)) {
            nodesByCluster.set(cluster, [])
        }
        nodesByCluster.get(cluster).push({ type: 'pipeline', node: pipeline })
    })

    allDataSources.forEach((ds) => {
        const cluster = ds.cluster || '_unclustered'
        if (!nodesByCluster.has(cluster)) {
            nodesByCluster.set(cluster, [])
        }
        nodesByCluster.get(cluster).push({ type: 'datasource', node: ds })
    })

    const rootClusters = []
    clusterDefinitions.forEach((cluster, name) => {
        if (!clusterHierarchy.has(name) && nodesByCluster.has(name)) {
            rootClusters.push(name)
        }
    })

    let colorIndex = 0

    function renderCluster(clusterName, depth = 0) {
        const cluster = clusterDefinitions.get(clusterName)
        if (!cluster) return ''

        const nodesInCluster = nodesByCluster.get(clusterName) || []
        const children = clusterChildren.get(clusterName) || []

        const hasNodes = nodesInCluster.length > 0
        const hasChildrenWithNodes = children.some(
            (child) => nodesByCluster.has(child) && nodesByCluster.get(child).length > 0
        )

        if (!hasNodes && !hasChildrenWithNodes) return ''

        const clusterColor = clusterColors[colorIndex % clusterColors.length]
        colorIndex++

        let result = `${'    '.repeat(depth + 1)}subgraph cluster_${clusterName.replace(/[^a-zA-Z0-9]/g, '_')} {
${'    '.repeat(depth + 2)}label="${clusterName}";
${'    '.repeat(depth + 2)}style="dotted";
${'    '.repeat(depth + 2)}color="#666666";
${'    '.repeat(depth + 2)}fontsize=11;
${'    '.repeat(depth + 2)}fontname="Arial";

`

        nodesInCluster.forEach((item) => {
            if (item.type === 'pipeline') {
                const pipeline = item.node
                const isGroup = pipeline._isGroup
                const memberCount = pipeline._members?.length || 0
                const label = isGroup
                    ? `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>`
                    : pipeline.schedule
                      ? `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${pipeline.schedule}</I></FONT>>`
                      : `"${pipeline.name}"`
                const fillColor = isGroup ? '#e0f2f1' : '#e3f2fd'
                const borderColor = isGroup ? '#00897b' : '#1976d2'
                const penWidth = isGroup ? '2' : '1'
                result += `${'    '.repeat(depth + 2)}"${pipeline.name}" [shape=box, style="filled,rounded",
${'    '.repeat(depth + 3)}fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
${'    '.repeat(depth + 3)}fontname="Arial",
${'    '.repeat(depth + 3)}label=${label}];
`
            } else if (item.type === 'datasource' && !state.pipelinesOnlyView) {
                const ds = item.node
                result += `${'    '.repeat(depth + 2)}"${ds.name}" [shape=ellipse, style=filled,
${'    '.repeat(depth + 3)}fillcolor="#f3e5f5", color="#7b1fa2",
${'    '.repeat(depth + 3)}fontname="Arial", fontsize=10];
`
            }
        })

        children.forEach((childName) => {
            result += renderCluster(childName, depth + 1)
        })

        result += `${'    '.repeat(depth + 1)}}

`
        return result
    }

    rootClusters.forEach((clusterName) => {
        dot += renderCluster(clusterName)
    })

    const unclusteredNodes = nodesByCluster.get('_unclustered') || []
    unclusteredNodes.forEach((item) => {
        if (item.type === 'pipeline') {
            const pipeline = item.node
            const isGroup = pipeline._isGroup
            const memberCount = pipeline._members?.length || 0
            const label = isGroup
                ? `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>`
                : pipeline.schedule
                  ? `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${pipeline.schedule}</I></FONT>>`
                  : `"${pipeline.name}"`
            const fillColor = isGroup ? '#e0f2f1' : '#e3f2fd'
            const borderColor = isGroup ? '#00897b' : '#1976d2'
            const penWidth = isGroup ? '2' : '1'
            dot += `    "${pipeline.name}" [shape=box, style="filled,rounded",
        fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
        fontname="Arial",
        label=${label}];
`
        } else if (item.type === 'datasource' && !state.pipelinesOnlyView) {
            const ds = item.node
            dot += `    "${ds.name}" [shape=ellipse, style=filled,
        fillcolor="#f3e5f5", color="#7b1fa2",
        fontname="Arial", fontsize=10];
`
        }
    })

    if (!state.pipelinesOnlyView) {
        dot += '\n'
        pipelines.forEach((pipeline) => {
            pipeline.input_sources?.forEach((source) => {
                dot += `    "${source}" -> "${pipeline.name}" [color="${edgeColor}", arrowsize=0.8];\n`
            })
            pipeline.output_sources?.forEach((source) => {
                dot += `    "${pipeline.name}" -> "${source}" [color="${edgeColor}", arrowsize=0.8];\n`
            })
        })
    }

    const validNodeNames = new Set(pipelines.map((p) => p.name))
    dot += '\n'
    pipelines.forEach((pipeline) => {
        pipeline.upstream_pipelines?.forEach((upstream) => {
            if (validNodeNames.has(upstream)) {
                dot += `    "${upstream}" -> "${pipeline.name}" [color="#ff6b35", style="solid", arrowsize=0.8];\n`
            }
        })
    })

    dot += '\n    overlap=false; splines=true;\n}'
    return dot
}

let graphTabListenerAdded = false

export function renderGraph() {
    if (!state.currentConfig) return

    const currentHash = getConfigHash(state.currentConfig)

    if (!graphTabListenerAdded) {
        document.getElementById('graph-tab').addEventListener('shown.bs.tab', function () {
            const newHash = getConfigHash(state.currentConfig)
            if (!state.graphviz) {
                setTimeout(initializeGraph, 100)
                state.lastRenderedConfigHash = newHash
            } else if (newHash !== state.lastRenderedConfigHash) {
                updateGraph()
                state.lastRenderedConfigHash = newHash
            }
        })
        graphTabListenerAdded = true
    }

    if (document.getElementById('graph-tab').classList.contains('active')) {
        setTimeout(initializeGraph, 100)
        state.lastRenderedConfigHash = currentHash
    }
}

export function initializeGraph() {
    try {
        document.getElementById('graph').innerHTML = ''
        state.graphviz = d3
            .select('#graph')
            .graphviz()
            .width(document.getElementById('graph').clientWidth)
            .height(500)
            .fit(true)
            .on('renderEnd', setupGraphInteractivity)
        updateGraph()
    } catch (error) {
        console.error('Graphviz initialization error:', error)
        document.getElementById('graph').innerHTML = `
            <div class="alert alert-warning m-3">
                <strong>Graph rendering issue detected.</strong><br>
                Please check the Raw DOT tab to see the generated code.<br>
                <small>Error: ${error.message}</small>
            </div>
        `
    }
}

let isRendering = false
let pendingUpdate = false
let renderTimeout = null

export function updateGraph() {
    if (!state.graphviz) return

    // If already rendering, mark that we need another update when done
    if (isRendering) {
        pendingUpdate = true
        return
    }

    const viewKey = getViewStateKey()
    let cached = state.viewStateCache.get(viewKey)

    let dotSrc
    if (cached?.dot) {
        dotSrc = cached.dot
    } else {
        dotSrc = generateGraphvizDot()
        if (!cached) {
            cached = { dot: dotSrc }
            addToViewCache(viewKey, cached)
        } else {
            cached.dot = dotSrc
        }
    }

    isRendering = true
    const container = d3.select('#graph')
    container.style('opacity', 0.6)

    // Safety timeout to reset rendering state if transition gets stuck
    if (renderTimeout) clearTimeout(renderTimeout)
    renderTimeout = setTimeout(() => {
        if (isRendering) {
            container.style('opacity', 1)
            isRendering = false
            if (pendingUpdate) {
                pendingUpdate = false
                updateGraph()
            }
        }
    }, 3000)

    state.graphviz
        .transition(() => d3.transition().duration(150))
        .renderDot(dotSrc)
        .on('end', () => {
            if (renderTimeout) clearTimeout(renderTimeout)
            container.style('opacity', 1)
            isRendering = false

            // If there's a pending update, do it now
            if (pendingUpdate) {
                pendingUpdate = false
                updateGraph()
            } else {
                setTimeout(() => precomputeAdjacentStates(), 100)
            }
        })
}

// Pre-compute DOT strings for likely next states (toggle each expanded/collapsed group)
function precomputeAdjacentStates() {
    if (!state.currentConfig?.pipelines) return

    // Find all group names
    const groupNames = []
    state.currentConfig.pipelines.forEach((p) => {
        if (p.group && !groupNames.includes(p.group)) groupNames.push(p.group)
    })

    // Snapshot current state to avoid race conditions
    const currentExpanded = new Set(state.expandedGroups)
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'

    // Process one group at a time using requestIdleCallback or setTimeout
    let index = 0
    function processNext() {
        if (index >= groupNames.length) return

        const groupName = groupNames[index++]
        const wasExpanded = currentExpanded.has(groupName)

        // Compute alternate expanded set
        const altExpanded = new Set(currentExpanded)
        if (wasExpanded) {
            altExpanded.delete(groupName)
        } else {
            altExpanded.add(groupName)
        }

        // Compute key without modifying state
        const altExpandedKey = Array.from(altExpanded).sort().join(',')
        const altKey = `${state.groupedView}|${state.pipelinesOnlyView}|${isDark}|${altExpandedKey}`

        if (!state.viewStateCache.has(altKey)) {
            // Temporarily swap state just for DOT generation
            const savedExpanded = state.expandedGroups
            state.expandedGroups = altExpanded
            const altDot = generateGraphvizDot()
            state.expandedGroups = savedExpanded
            addToViewCache(altKey, { dot: altDot })
        }

        // Schedule next group with a small delay to avoid blocking
        if (index < groupNames.length) {
            setTimeout(processNext, 10)
        }
    }

    processNext()
}

export function setupGraphInteractivity() {
    const viewKey = getViewStateKey()
    let cached = state.viewStateCache.get(viewKey)

    // Check if we have cached lineage data for this view state
    // Also verify the cache has actual data (not empty from initEnd)
    if (cached?.lineage && Object.keys(cached.lineage.nodeLineage).length > 0) {
        // Restore cached lineage maps
        state.cachedUpstreamMap = cached.lineage.upstreamMap
        state.cachedDownstreamMap = cached.lineage.downstreamMap
        state.cachedLineage = cached.lineage.nodeLineage
    } else {
        // Build lineage maps by parsing the rendered graph
        state.cachedUpstreamMap = {}
        state.cachedDownstreamMap = {}
        state.cachedLineage = {}

        d3.select('#graph')
            .selectAll('.edge')
            .each(function () {
                const edge = d3.select(this)
                const title = edge.select('title').text()
                const match = title.match(/^(.+?)(?:->|--)\s*(.+?)$/)
                if (match) {
                    const source = match[1]
                    const target = match[2]
                    if (!state.cachedDownstreamMap[source]) state.cachedDownstreamMap[source] = []
                    if (!state.cachedUpstreamMap[target]) state.cachedUpstreamMap[target] = []
                    state.cachedDownstreamMap[source].push(target)
                    state.cachedUpstreamMap[target].push(source)
                }
            })

        function getFullChain(node, map, visited = new Set(), depth = 1) {
            if (visited.has(node)) return []
            visited.add(node)
            const neighbors = map[node] || []
            let result = []
            neighbors.forEach((n) => {
                result.push({ name: n, depth: depth })
                result.push(...getFullChain(n, map, visited, depth + 1))
            })
            return result
        }

        d3.select('#graph')
            .selectAll('.node')
            .each(function () {
                const nodeName = d3.select(this).select('title').text()
                state.cachedLineage[nodeName] = {
                    upstream: getFullChain(nodeName, state.cachedUpstreamMap, new Set()),
                    downstream: getFullChain(nodeName, state.cachedDownstreamMap, new Set())
                }
            })

        // Cache the computed lineage data (only if we actually have nodes)
        if (Object.keys(state.cachedLineage).length > 0) {
            if (!cached) {
                cached = {
                    lineage: {
                        upstreamMap: state.cachedUpstreamMap,
                        downstreamMap: state.cachedDownstreamMap,
                        nodeLineage: state.cachedLineage
                    }
                }
                addToViewCache(viewKey, cached)
            } else {
                cached.lineage = {
                    upstreamMap: state.cachedUpstreamMap,
                    downstreamMap: state.cachedDownstreamMap,
                    nodeLineage: state.cachedLineage
                }
            }
        }
    }

    if (document.getElementById('node-tooltip')) {
        document.getElementById('node-tooltip').remove()
    }
    const tooltip = document.createElement('div')
    tooltip.id = 'node-tooltip'
    tooltip.className = 'node-tooltip'
    tooltip.style.display = 'none'
    document.body.appendChild(tooltip)

    d3.select('#graph')
        .selectAll('.node')
        .style('cursor', 'pointer')
        .on('click', function (event, d) {
            event.stopPropagation()
            const nodeName = d3.select(this).select('title').text()
            selectNode(nodeName, this)
        })
        .on('dblclick', function (event, d) {
            event.stopPropagation()
            const nodeName = d3.select(this).select('title').text()
            if (state.groupedView && state.currentConfig?.pipelines) {
                const isGroupNode = state.currentConfig.pipelines.some((p) => p.group === nodeName)
                if (isGroupNode) {
                    toggleGroup(nodeName)
                }
            }
        })
        .on('mouseover', function (event, d) {
            const nodeName = d3.select(this).select('title').text()
            showNodeTooltip(event, nodeName)
        })
        .on('mousemove', function (event, d) {
            tooltip.style.left = event.pageX + 10 + 'px'
            tooltip.style.top = event.pageY - 10 + 'px'
        })
        .on('mouseout', function (event, d) {
            hideNodeTooltip()
        })

    d3.select('#graph').on('click', function (event) {
        if (
            event.target.tagName === 'svg' ||
            event.target === event.currentTarget ||
            event.target.classList.contains('graph-container') ||
            !event.target.closest('.node')
        ) {
            clearSelection()
        }
    })
}

export function showNodeTooltip(event, nodeName) {
    const tooltip = document.getElementById('node-tooltip')
    if (!tooltip || !state.currentConfig) return

    let content = ''

    if (state.groupedView) {
        const groupMembers = state.currentConfig.pipelines?.filter((p) => p.group === nodeName)
        if (groupMembers?.length > 0) {
            content = `${nodeName} (${groupMembers.length} pipelines)`
            content += `\nClick for details`
            content += `\nMembers: ${groupMembers.map((p) => p.name).join(', ')}`
            tooltip.textContent = content
            tooltip.style.display = 'block'
            tooltip.style.left = event.pageX + 10 + 'px'
            tooltip.style.top = event.pageY - 10 + 'px'
            return
        }
    }

    const pipeline = state.currentConfig.pipelines?.find((p) => p.name === nodeName)
    if (pipeline) {
        content = `${pipeline.name}`
        if (pipeline.description) content += `\nDescription: ${pipeline.description}`
        if (pipeline.schedule) content += `\nSchedule: ${pipeline.schedule}`
        if (pipeline.cluster) content += `\nCluster: ${pipeline.cluster}`
        if (pipeline.tags && pipeline.tags.length > 0) content += `\nTags: ${pipeline.tags.join(', ')}`
        if (pipeline.input_sources && pipeline.input_sources.length > 0)
            content += `\nInputs: ${pipeline.input_sources.join(', ')}`
        if (pipeline.output_sources && pipeline.output_sources.length > 0)
            content += `\nOutputs: ${pipeline.output_sources.join(', ')}`
    }

    const datasource = state.currentConfig.datasources?.find((ds) => ds.name === nodeName)
    if (datasource) {
        content = `${datasource.name}`
        if (datasource.type) content += `\nType: ${datasource.type.toUpperCase()}`
        if (datasource.description) content += `\nDescription: ${datasource.description}`
        if (datasource.owner) content += `\nOwner: ${datasource.owner}`
        if (datasource.cluster) content += `\nCluster: ${datasource.cluster}`
        if (datasource.tags && datasource.tags.length > 0) content += `\nTags: ${datasource.tags.join(', ')}`

        if (datasource.metadata) {
            const keyFields = ['size', 'record_count', 'refresh_frequency', 'environment']
            keyFields.forEach((field) => {
                if (datasource.metadata[field]) {
                    content += `\n${field.replace(/_/g, ' ')}: ${datasource.metadata[field]}`
                }
            })
        }
    }

    if (content) {
        tooltip.textContent = content
        tooltip.style.display = 'block'
        tooltip.style.left = event.pageX + 10 + 'px'
        tooltip.style.top = event.pageY - 10 + 'px'
    }
}

export function hideNodeTooltip() {
    const tooltip = document.getElementById('node-tooltip')
    if (tooltip) {
        tooltip.style.display = 'none'
    }
}

export function selectNode(nodeName, nodeElement) {
    state.selectedNode = nodeName
    clearHighlights()
    d3.select(nodeElement).classed('node-highlighted', true)

    const lineage = state.cachedLineage[nodeName] || { upstream: [], downstream: [] }
    const upstream = lineage.upstream
    const downstream = lineage.downstream
    const allConnected = new Set([...upstream.map((x) => x.name), ...downstream.map((x) => x.name)])

    d3.select('#graph')
        .selectAll('.node')
        .each(function () {
            const node = d3.select(this)
            const nodeTitle = node.select('title').text()
            if (allConnected.has(nodeTitle)) {
                node.classed('node-connected', true)
            } else if (nodeTitle !== nodeName) {
                node.classed('node-dimmed', true)
            }
        })

    d3.select('#graph')
        .selectAll('.edge')
        .each(function () {
            const edge = d3.select(this)
            const title = edge.select('title').text()
            const match = title.match(/^(.+?)(?:->|--)\s*(.+?)$/)
            if (match) {
                const source = match[1]
                const target = match[2]
                const sourceInChain = source === nodeName || allConnected.has(source)
                const targetInChain = target === nodeName || allConnected.has(target)
                if (sourceInChain && targetInChain) {
                    edge.classed('edge-highlighted', true)
                } else {
                    edge.classed('edge-dimmed', true)
                }
            }
        })

    showNodeDetails(nodeName, upstream, downstream)
}

export function showNodeDetails(nodeName, upstream = [], downstream = []) {
    const col = document.getElementById('node-details-col')
    const content = document.getElementById('node-details-content')

    if (!state.currentConfig) return

    let html = ''
    let nodeData = null
    let nodeType = ''

    const pipeline = state.currentConfig.pipelines?.find((p) => p.name === nodeName)
    if (pipeline) {
        nodeData = pipeline
        nodeType = 'Pipeline'
    }

    if (!nodeData && state.groupedView) {
        const groupMembers = state.currentConfig.pipelines?.filter((p) => p.group === nodeName)
        if (groupMembers?.length > 0) {
            const allInputs = new Set()
            const allOutputs = new Set()
            groupMembers.forEach((p) => {
                p.input_sources?.forEach((s) => allInputs.add(s))
                p.output_sources?.forEach((s) => allOutputs.add(s))
            })
            nodeData = {
                name: nodeName,
                description: `Group containing ${groupMembers.length} pipelines`,
                input_sources: Array.from(allInputs),
                output_sources: Array.from(allOutputs),
                cluster: groupMembers[0].cluster,
                _members: groupMembers.map((p) => p.name)
            }
            nodeType = 'Pipeline Group'
        }
    }

    const datasource = state.currentConfig.datasources?.find((ds) => ds.name === nodeName)
    if (datasource) {
        nodeData = datasource
        nodeType = 'Data Source'
    }

    if (!nodeData) {
        const allSources = new Set()
        state.currentConfig.pipelines?.forEach((p) => {
            p.input_sources?.forEach((s) => allSources.add(s))
            p.output_sources?.forEach((s) => allSources.add(s))
        })
        if (allSources.has(nodeName)) {
            nodeData = { name: nodeName, description: 'Auto-created from pipeline references' }
            nodeType = 'Data Source'
        }
    }

    if (!nodeData) {
        col.style.display = 'none'
        return
    }

    html = `<h5>${nodeData.name}</h5>`
    html += `<div class="detail-label">Type</div>`
    html += `<div class="detail-value"><span class="badge bg-secondary">${nodeType}</span></div>`

    // Blast Radius button - always show for any node
    html += `<div class="mt-2 mb-2">
        <button class="btn btn-sm btn-outline-danger" onclick="showBlastRadius('${nodeName.replace(/'/g, "\\'")}')">
            ◉ Blast Radius
        </button>
    </div>`

    if (nodeType === 'Pipeline Group') {
        const isExpanded = state.expandedGroups.has(nodeName)
        html += `<div class="mt-2 mb-2">
            <button class="btn btn-sm btn-outline-warning" onclick="toggleGroup('${nodeName}')">
                ${isExpanded ? 'Collapse Group' : 'Expand Group'}
            </button>
        </div>`
    }

    if (nodeType === 'Pipeline' && nodeData.group && state.expandedGroups.has(nodeData.group)) {
        html += `<div class="mt-2 mb-2">
            <button class="btn btn-sm btn-outline-secondary" onclick="toggleGroup('${nodeData.group}')">
                Collapse Group (${nodeData.group})
            </button>
        </div>`
    }

    if (nodeData.description) {
        html += `<div class="detail-label">Description</div>`
        html += `<div class="detail-value">${nodeData.description}</div>`
    }

    if (nodeData.schedule) {
        html += `<div class="detail-label">Schedule</div>`
        html += `<div class="detail-value"><code class="text-success">${nodeData.schedule}</code></div>`
    }

    if (nodeData.type) {
        html += `<div class="detail-label">Source Type</div>`
        html += `<div class="detail-value"><span class="badge badge-${nodeData.type}">${nodeData.type.toUpperCase()}</span></div>`
    }

    if (nodeData.owner) {
        html += `<div class="detail-label">Owner</div>`
        html += `<div class="detail-value">${nodeData.owner}</div>`
    }

    if (nodeData.cluster) {
        html += `<div class="detail-label">Cluster</div>`
        html += `<div class="detail-value"><span class="badge badge-cluster">${nodeData.cluster}</span></div>`
    }

    if (nodeData.input_sources?.length) {
        html += `<div class="detail-label">Input Sources</div>`
        html += `<div class="detail-value">${nodeData.input_sources
            .map((s) => `<span class="badge me-1 mb-1" style="background-color: #e3f2fd; color: #1565c0;">${s}</span>`)
            .join('')}</div>`
    }

    if (nodeData.output_sources?.length) {
        html += `<div class="detail-label">Output Sources</div>`
        html += `<div class="detail-value">${nodeData.output_sources
            .map((s) => `<span class="badge me-1 mb-1" style="background-color: #e8f5e8; color: #2e7d32;">${s}</span>`)
            .join('')}</div>`
    }

    if (nodeData.upstream_pipelines?.length) {
        html += `<div class="detail-label">Upstream Pipelines</div>`
        html += `<div class="detail-value">${nodeData.upstream_pipelines
            .map((p) => `<span class="badge me-1 mb-1" style="background-color: #fff3e0; color: #e65100;">${p}</span>`)
            .join('')}</div>`
    }

    if (nodeData._members?.length) {
        html += `<div class="detail-label">Member Pipelines</div>`
        html += `<div class="detail-value">${nodeData._members
            .map((p) => `<span class="badge me-1 mb-1" style="background-color: #e0f2f1; color: #00897b;">${p}</span>`)
            .join('')}</div>`
    }

    if (nodeData.tags?.length) {
        html += `<div class="detail-label">Tags</div>`
        html += `<div class="detail-value">${nodeData.tags
            .map((t) => `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404;">${t}</span>`)
            .join('')}</div>`
    }

    if (nodeData.metadata && Object.keys(nodeData.metadata).length) {
        html += `<div class="detail-label">Metadata</div>`
        html += `<div class="detail-value">`
        Object.entries(nodeData.metadata).forEach(([key, value]) => {
            html += `<div class="small"><strong>${key.replace(/_/g, ' ')}:</strong> ${value}</div>`
        })
        html += `</div>`
    }

    if (nodeData.links && Object.keys(nodeData.links).length) {
        html += `<div class="links-section">`
        html += `<div class="detail-label">Links</div>`
        Object.entries(nodeData.links).forEach(([name, url]) => {
            html += `<a href="${url}" target="_blank" class="btn btn-sm btn-outline-primary link-btn">${name}</a>`
        })
        html += `</div>`
    }

    const pipelineNames = new Set((state.currentConfig.pipelines || []).map((p) => p.name))
    if (state.groupedView) {
        ;(state.currentConfig.pipelines || []).forEach((p) => {
            if (p.group) pipelineNames.add(p.group)
        })
    }

    function dedupeAndSort(items, filterFn) {
        const seen = new Map()
        items
            .filter((x) => filterFn(x.name))
            .forEach((x) => {
                if (!seen.has(x.name) || seen.get(x.name).depth > x.depth) {
                    seen.set(x.name, x)
                }
            })
        return [...seen.values()].sort((a, b) => a.depth - b.depth)
    }

    const upstreamPipelines = dedupeAndSort(upstream, (n) => pipelineNames.has(n))
    const upstreamSources = dedupeAndSort(upstream, (n) => !pipelineNames.has(n))
    const downstreamPipelines = dedupeAndSort(downstream, (n) => pipelineNames.has(n))
    const downstreamSources = dedupeAndSort(downstream, (n) => !pipelineNames.has(n))

    function renderLineageList(items, label) {
        if (items.length === 0) return ''
        let out = `<div class="detail-label">${label} (${items.length})</div><div class="detail-value">`
        items.forEach((x) => {
            const indent = (x.depth - 1) * 12
            const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15)
            const prefix = x.depth > 1 ? '└ ' : ''
            out += `<div class="lineage-link" data-node-name="${x.name}" style="padding-left: ${indent}px; opacity: ${opacity};">${prefix}${x.name}</div>`
        })
        out += `</div>`
        return out
    }

    function getNodeMetadata(name) {
        const pipeline = state.currentConfig.pipelines?.find((p) => p.name === name)
        if (pipeline) {
            const meta = { name, type: 'pipeline', depth: undefined }
            if (pipeline.description) meta.description = pipeline.description
            if (pipeline.schedule) meta.schedule = pipeline.schedule
            if (pipeline.cluster) meta.cluster = pipeline.cluster
            if (pipeline.group) meta.group = pipeline.group
            if (pipeline.owner) meta.owner = pipeline.owner
            if (pipeline.tags?.length) meta.tags = pipeline.tags
            if (pipeline.input_sources?.length) meta.input_sources = pipeline.input_sources
            if (pipeline.output_sources?.length) meta.output_sources = pipeline.output_sources
            if (pipeline.upstream_pipelines?.length) meta.upstream_pipelines = pipeline.upstream_pipelines
            if (pipeline.links && Object.keys(pipeline.links).length) meta.links = pipeline.links
            if (pipeline.metadata && Object.keys(pipeline.metadata).length) meta.metadata = pipeline.metadata
            return meta
        }
        const datasource = state.currentConfig.datasources?.find((ds) => ds.name === name)
        if (datasource) {
            const meta = { name, type: 'datasource', depth: undefined }
            if (datasource.description) meta.description = datasource.description
            if (datasource.type) meta.source_type = datasource.type
            if (datasource.cluster) meta.cluster = datasource.cluster
            if (datasource.owner) meta.owner = datasource.owner
            if (datasource.tags?.length) meta.tags = datasource.tags
            if (datasource.links && Object.keys(datasource.links).length) meta.links = datasource.links
            if (datasource.metadata && Object.keys(datasource.metadata).length) meta.metadata = datasource.metadata
            return meta
        }
        return { name, type: 'datasource' }
    }

    const allUpstreamNodes = new Set([...upstreamPipelines, ...upstreamSources].map((x) => x.name))
    const allDownstreamNodes = new Set([...downstreamPipelines, ...downstreamSources].map((x) => x.name))

    const upstreamEdges = []
    allUpstreamNodes.forEach((node) => {
        const downstreamNeighbors = state.cachedDownstreamMap[node] || []
        downstreamNeighbors.forEach((neighbor) => {
            if (allUpstreamNodes.has(neighbor) || neighbor === nodeName) {
                upstreamEdges.push({ from: node, to: neighbor })
            }
        })
    })

    const downstreamEdges = []
    downstreamEdges.push(
        ...(state.cachedDownstreamMap[nodeName] || [])
            .filter((n) => allDownstreamNodes.has(n))
            .map((n) => ({ from: nodeName, to: n }))
    )
    allDownstreamNodes.forEach((node) => {
        const downstreamNeighbors = state.cachedDownstreamMap[node] || []
        downstreamNeighbors.forEach((neighbor) => {
            if (allDownstreamNodes.has(neighbor)) {
                downstreamEdges.push({ from: node, to: neighbor })
            }
        })
    })

    const lineageJson = {
        node: nodeName,
        type: nodeType,
        ...getNodeMetadata(nodeName),
        upstream: {
            pipelines: upstreamPipelines.map((x) => ({ ...getNodeMetadata(x.name), depth: x.depth })),
            sources: upstreamSources.map((x) => ({ ...getNodeMetadata(x.name), depth: x.depth })),
            edges: upstreamEdges
        },
        downstream: {
            pipelines: downstreamPipelines.map((x) => ({ ...getNodeMetadata(x.name), depth: x.depth })),
            sources: downstreamSources.map((x) => ({ ...getNodeMetadata(x.name), depth: x.depth })),
            edges: downstreamEdges
        }
    }

    const hasLineage =
        upstreamPipelines.length > 0 ||
        upstreamSources.length > 0 ||
        downstreamPipelines.length > 0 ||
        downstreamSources.length > 0

    if (hasLineage) {
        html += `<div class="lineage-view-toggle">
            <span class="lineage-toggle-label active" data-view="tree">Tree</span>
            <div class="lineage-toggle-slider"></div>
            <span class="lineage-toggle-label" data-view="json">JSON</span>
        </div>`
    }

    html += `<div class="lineage-tree-view">`
    html += renderLineageList(upstreamPipelines, 'UPSTREAM PIPELINES')
    html += renderLineageList(upstreamSources, 'UPSTREAM SOURCES')
    html += renderLineageList(downstreamPipelines, 'DOWNSTREAM PIPELINES')
    html += renderLineageList(downstreamSources, 'DOWNSTREAM SOURCES')
    html += `</div>`

    html += `<div class="lineage-json-view" style="display: none;">
        <div class="detail-label">LINEAGE JSON</div>
        <pre class="lineage-json-pre">${JSON.stringify(lineageJson, null, 2)}</pre>
    </div>`

    const graphCol = document.getElementById('graph-col')
    graphCol.classList.remove('col-md-12')
    graphCol.classList.add('col-md-8')
    col.style.display = 'block'
    content.innerHTML = html

    const slider = content.querySelector('.lineage-toggle-slider')
    if (slider) {
        slider.addEventListener('click', function () {
            const isJson = this.classList.toggle('json-active')
            const treeView = content.querySelector('.lineage-tree-view')
            const jsonView = content.querySelector('.lineage-json-view')
            const treeLabel = content.querySelector('.lineage-toggle-label[data-view="tree"]')
            const jsonLabel = content.querySelector('.lineage-toggle-label[data-view="json"]')

            if (isJson) {
                treeView.style.display = 'none'
                jsonView.style.display = 'block'
                treeLabel.classList.remove('active')
                jsonLabel.classList.add('active')
            } else {
                treeView.style.display = 'block'
                jsonView.style.display = 'none'
                treeLabel.classList.add('active')
                jsonLabel.classList.remove('active')
            }
        })
    }

    content.querySelectorAll('.lineage-link').forEach((el) => {
        el.addEventListener('click', function () {
            const targetName = this.getAttribute('data-node-name')
            if (targetName) {
                d3.select('#graph')
                    .selectAll('.node')
                    .each(function () {
                        const nodeTitle = d3.select(this).select('title').text()
                        if (nodeTitle === targetName) {
                            selectNode(targetName, this)
                        }
                    })
            }
        })
    })
}

export function clearSelection() {
    state.selectedNode = null
    clearHighlights()
    const col = document.getElementById('node-details-col')
    const graphCol = document.getElementById('graph-col')
    if (col) col.style.display = 'none'
    if (graphCol) {
        graphCol.classList.remove('col-md-8')
        graphCol.classList.add('col-md-12')
    }
}

export function clearHighlights() {
    d3.select('#graph').selectAll('.node').classed('node-highlighted node-connected node-dimmed', false)
    d3.select('#graph').selectAll('.edge').classed('edge-highlighted edge-dimmed', false)
}

export function fuzzyMatch(text, query) {
    text = text.toLowerCase()
    query = query.toLowerCase()

    if (text.includes(query)) {
        return { match: true, score: query.length / text.length + 0.5 }
    }

    let queryIdx = 0
    let score = 0
    let lastMatchIdx = -1

    for (let i = 0; i < text.length && queryIdx < query.length; i++) {
        if (text[i] === query[queryIdx]) {
            score += 1
            // Bonus for consecutive matches
            if (lastMatchIdx === i - 1) score += 0.5
            // Bonus for matching at word boundaries
            if (i === 0 || text[i - 1] === '_' || text[i - 1] === '-' || text[i - 1] === ' ') score += 0.3
            lastMatchIdx = i
            queryIdx++
        }
    }

    if (queryIdx === query.length) {
        return { match: true, score: score / text.length }
    }
    return { match: false, score: 0 }
}

export function highlightMatch(text, query) {
    const lowerText = text.toLowerCase()
    const lowerQuery = query.toLowerCase()

    const idx = lowerText.indexOf(lowerQuery)
    if (idx !== -1) {
        return (
            text.substring(0, idx) +
            '<span class="result-match">' +
            text.substring(idx, idx + query.length) +
            '</span>' +
            text.substring(idx + query.length)
        )
    }

    let result = ''
    let queryIdx = 0
    for (let i = 0; i < text.length; i++) {
        if (queryIdx < query.length && text[i].toLowerCase() === lowerQuery[queryIdx]) {
            result += '<span class="result-match">' + text[i] + '</span>'
            queryIdx++
        } else {
            result += text[i]
        }
    }
    return result
}

export function searchNodes(event) {
    const dropdown = document.getElementById('graph-search-results')
    const items = dropdown.querySelectorAll('.search-result-item[data-name]')

    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault()
        if (items.length === 0) return

        const current = dropdown.querySelector('.search-result-item.selected')
        let index = current ? Array.from(items).indexOf(current) : -1

        if (event.key === 'ArrowDown') {
            index = index < items.length - 1 ? index + 1 : 0
        } else {
            index = index > 0 ? index - 1 : items.length - 1
        }

        items.forEach((item) => item.classList.remove('selected'))
        items[index].classList.add('selected')
        items[index].scrollIntoView({ block: 'nearest' })
        return
    }

    if (event.key === 'Enter') {
        const selected = dropdown.querySelector('.search-result-item.selected')
        if (selected && selected.dataset.name) {
            selectSearchResult(selected.dataset.name)
        }
        return
    }

    if (event.key === 'Escape') {
        dropdown.classList.remove('show')
        dropdown.innerHTML = ''
        return
    }

    const query = event.target.value.trim()

    if (!query || query.length < 1 || !state.currentConfig) {
        dropdown.classList.remove('show')
        dropdown.innerHTML = ''
        return
    }

    const results = []

    state.currentConfig.pipelines?.forEach((p) => {
        const nameMatch = fuzzyMatch(p.name, query)
        const descMatch = p.description ? fuzzyMatch(p.description, query) : { match: false, score: 0 }
        if (nameMatch.match || descMatch.match) {
            results.push({
                name: p.name,
                type: 'pipeline',
                score: Math.max(nameMatch.score * 1.5, descMatch.score)
            })
        }
    })

    state.currentConfig.datasources?.forEach((ds) => {
        const nameMatch = fuzzyMatch(ds.name, query)
        const descMatch = ds.description ? fuzzyMatch(ds.description, query) : { match: false, score: 0 }
        if (nameMatch.match || descMatch.match) {
            results.push({
                name: ds.name,
                type: 'datasource',
                score: Math.max(nameMatch.score * 1.5, descMatch.score)
            })
        }
    })

    const autoSources = new Set()
    state.currentConfig.pipelines?.forEach((p) => {
        p.input_sources?.forEach((s) => autoSources.add(s))
        p.output_sources?.forEach((s) => autoSources.add(s))
    })
    autoSources.forEach((name) => {
        if (!state.currentConfig.datasources?.find((ds) => ds.name === name)) {
            const nameMatch = fuzzyMatch(name, query)
            if (nameMatch.match) {
                results.push({
                    name: name,
                    type: 'datasource',
                    score: nameMatch.score * 1.5
                })
            }
        }
    })

    results.sort((a, b) => b.score - a.score)
    const topResults = results.slice(0, 8)

    if (topResults.length === 0) {
        dropdown.innerHTML = '<div class="search-result-item text-muted">No matches found</div>'
        dropdown.classList.add('show')
        return
    }

    dropdown.innerHTML = topResults
        .map(
            (r, i) => `
        <div class="search-result-item${i === 0 ? ' selected' : ''}" data-name="${r.name}" onclick="selectSearchResult('${r.name}')">
            <span class="result-type ${r.type}">${r.type === 'pipeline' ? 'Pipeline' : 'Source'}</span>
            <span class="result-name">${highlightMatch(r.name, query)}</span>
        </div>
    `
        )
        .join('')
    dropdown.classList.add('show')
}

export function selectSearchResult(nodeName) {
    const dropdown = document.getElementById('graph-search-results')
    const searchInput = document.getElementById('graph-search')
    dropdown.classList.remove('show')
    searchInput.value = ''

    let found = false
    d3.select('#graph')
        .selectAll('.node')
        .each(function () {
            const node = d3.select(this)
            const nodeTitle = node.select('title').text()
            if (nodeTitle === nodeName) {
                found = true
                selectNode(nodeName, this)
            }
        })

    if (!found) {
        showNodeDetails(nodeName)
    }
}

document.addEventListener('click', function (e) {
    const dropdown = document.getElementById('graph-search-results')
    const searchInput = document.getElementById('graph-search')
    if (dropdown && !dropdown.contains(e.target) && e.target !== searchInput) {
        dropdown.classList.remove('show')
    }
})

export function fitGraph() {
    if (state.graphviz) state.graphviz.fit(true)
}

export function resetGraph() {
    if (state.graphviz) state.graphviz.resetZoom()
}

export function collapseAllGroups() {
    state.expandedGroups.clear()
    updateGraph()
}

export function togglePipelinesOnly() {
    state.pipelinesOnlyView = !state.pipelinesOnlyView
    const btn = document.getElementById('pipelines-only-btn')
    if (state.pipelinesOnlyView) {
        btn.classList.remove('btn-outline-secondary')
        btn.classList.add('btn-secondary')
    } else {
        btn.classList.remove('btn-secondary')
        btn.classList.add('btn-outline-secondary')
    }
    updateGraph()
}

export function toggleGroup(groupName) {
    if (state.expandedGroups.has(groupName)) {
        state.expandedGroups.delete(groupName)
    } else {
        state.expandedGroups.add(groupName)
    }
    updateGraph()
    showNodeDetails(groupName)
}

export function showBlastRadius(nodeName) {
    const modal = document.getElementById('blastRadiusModal')
    const graphContainer = document.getElementById('blast-radius-graph')
    const summaryContainer = document.getElementById('blast-radius-summary')
    const nodeNameEl = document.getElementById('blast-radius-node-name')

    if (!modal || !graphContainer || !summaryContainer) return

    // Reset the graph container completely to avoid zoom issues
    graphContainer.innerHTML = ''
    blastRadiusGraphInstance = null

    nodeNameEl.textContent = nodeName

    const analysis = generateBlastRadiusAnalysis(nodeName)

    if (!analysis || analysis.downstream.length === 0) {
        summaryContainer.innerHTML = `
            <div class="text-center" style="padding: 2rem; color: var(--text-muted);">
                <div style="font-size: 2rem; margin-bottom: 1rem;">✓</div>
                <div>No downstream dependencies</div>
                <div class="small mt-2">This node has no downstream impact.</div>
            </div>
        `
        graphContainer.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: center; height: 100%; color: var(--text-muted);">
                No downstream dependencies to visualize
            </div>
        `
        // Show modal
        const bsModal = new bootstrap.Modal(modal)
        bsModal.show()
        return
    }

    // Build summary HTML with Tree/JSON toggle
    const isGroup = analysis.source_type === 'group'
    let summaryHtml = `
        <div class="lineage-view-toggle" style="margin-bottom: 0.75rem;">
            <span class="lineage-toggle-label active" data-view="tree">Summary</span>
            <div class="lineage-toggle-slider" id="blast-radius-toggle"></div>
            <span class="lineage-toggle-label" data-view="json">JSON</span>
        </div>
        <div id="blast-radius-tree-view">
    `

    // Show group members if this is a group
    if (isGroup && analysis.group_members) {
        summaryHtml += `
            <div class="mb-3">
                <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;">Group Members (${analysis.group_size})</div>
                <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
                    ${analysis.group_members.map((m) => `<span style="font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid #00897b;">▢ ${m}</span>`).join('')}
                </div>
            </div>
        `
    }

    summaryHtml += `
            <div class="mb-3">
                <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;">Impact Summary</div>
                <div style="display: flex; gap: 1rem;">
                    <div style="text-align: center; padding: 0.5rem 1rem; background: var(--bg-secondary); border-radius: 4px;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #c98b8b;">${analysis.total_affected}</div>
                        <div style="font-size: 0.7rem; color: var(--text-muted);">Total Affected</div>
                    </div>
                    <div style="text-align: center; padding: 0.5rem 1rem; background: var(--bg-secondary); border-radius: 4px;">
                        <div style="font-size: 1.5rem; font-weight: bold; color: #6b9dc4;">${analysis.max_depth}</div>
                        <div style="font-size: 0.7rem; color: var(--text-muted);">Max Depth</div>
                    </div>
                </div>
            </div>
    `

    // Affected nodes by depth
    Object.entries(analysis.by_depth).forEach(([depth, nodes]) => {
        const depthColors = ['#c98b8b', '#d4a574', '#c4c474', '#7cb47c', '#6b9dc4', '#a88bc4']
        const color = depthColors[Math.min(parseInt(depth), depthColors.length - 1)]

        summaryHtml += `
            <div class="mb-2">
                <div style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;">
                    Depth ${depth} <span style="color: ${color};">●</span>
                </div>
                <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
        `
        nodes.forEach((node) => {
            const icon = node.type === 'pipeline' ? '▢' : '○'
            summaryHtml += `<span style="font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid ${color};">${icon} ${node.name}</span>`
        })
        summaryHtml += `</div></div>`
    })

    summaryHtml += `</div>`

    // JSON view
    summaryHtml += `
        <div id="blast-radius-json-view" style="display: none;">
            <pre style="font-size: 0.7rem; max-height: 400px; overflow: auto; background: var(--bg-code); color: var(--text-primary); padding: 0.75rem; border-radius: 4px; margin: 0; border: 1px solid var(--border-color);">${JSON.stringify(analysis, null, 2)}</pre>
        </div>
    `

    summaryContainer.innerHTML = summaryHtml

    // Set up toggle listener
    const toggle = document.getElementById('blast-radius-toggle')
    if (toggle) {
        toggle.addEventListener('click', function () {
            const isJson = this.classList.toggle('json-active')
            const treeView = document.getElementById('blast-radius-tree-view')
            const jsonView = document.getElementById('blast-radius-json-view')
            const treeLabel = summaryContainer.querySelector('.lineage-toggle-label[data-view="tree"]')
            const jsonLabel = summaryContainer.querySelector('.lineage-toggle-label[data-view="json"]')

            if (isJson) {
                treeView.style.display = 'none'
                jsonView.style.display = 'block'
                treeLabel.classList.remove('active')
                jsonLabel.classList.add('active')
            } else {
                treeView.style.display = 'block'
                jsonView.style.display = 'none'
                treeLabel.classList.add('active')
                jsonLabel.classList.remove('active')
            }
        })
    }

    // Render the graph - wait for modal to be shown first for proper sizing
    const bsModal = new bootstrap.Modal(modal)

    modal.addEventListener(
        'shown.bs.modal',
        function onShown() {
            modal.removeEventListener('shown.bs.modal', onShown)

            const dot = generateBlastRadiusDot(analysis)
            if (dot) {
                try {
                    graphContainer.innerHTML = ''
                    const width = graphContainer.clientWidth || 600
                    const height = 500

                    blastRadiusGraphInstance = d3
                        .select('#blast-radius-graph')
                        .graphviz()
                        .width(width)
                        .height(height)
                        .fit(true)
                        .zoom(false) // Disable built-in zoom, we'll add our own
                        .on('end', function () {
                            // Add manual zoom behavior after render
                            const svg = d3.select('#blast-radius-graph svg')
                            const g = svg.select('g')

                            const zoom = d3
                                .zoom()
                                .scaleExtent([0.1, 4])
                                .on('zoom', function (event) {
                                    g.attr('transform', event.transform)
                                })

                            svg.call(zoom)

                            // Add reset button behavior
                            svg.on('dblclick.zoom', function () {
                                svg.transition().duration(300).call(zoom.transform, d3.zoomIdentity)
                            })
                        })

                    blastRadiusGraphInstance.renderDot(dot)
                } catch (e) {
                    console.error('Blast radius graph error:', e)
                    graphContainer.innerHTML = `
                    <div style="display: flex; align-items: center; justify-content: center; height: 100%; color: var(--text-muted);">
                        Error rendering graph
                    </div>
                `
                }
            }
        },
        { once: true }
    )

    bsModal.show()
}
