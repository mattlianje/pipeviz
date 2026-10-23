import { state, getConfigHash, getViewStateKey, clearViewStateCache, addToViewCache } from './state.js'
import { renderAttributeGraph } from './attributes.js'
import { generateBlastRadiusAnalysis, generateBlastRadiusDot } from './blastradius.js'
import { getCriticalPathEdges, getCriticalPathNodes, getCostliestPathEdges, getCostliestPathNodes } from './stats.js'
import {
    updateHashWithNode,
    getNodeFromHash,
    updateHashWithBlast,
    clearBlastFromHash,
    getBlastFromHash
} from './tabs.js'
import { formatSchedule } from './cron.js'
import { escapeHtml, jsArg, safeUrl, escapeDot } from './escape.js'
import { getLineageModel, litLineage } from './lineage.js'
import { updateLineageControl } from './lineagectl.js'

let blastRadiusGraphInstance = null

// Past this many nodes: polyline edges, capped layout, no morph transitions,
// and the laid-out SVG is cached.
const BIG_GRAPH_THRESHOLD = 250
const ZOOM_EXTENT = [0.02, 2000]

const LINK_ICON =
    '<svg class="detail-link-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 17H7A5 5 0 0 1 7 7h2"/><path d="M15 7h2a5 5 0 1 1 0 10h-2"/><line x1="8" y1="12" x2="16" y2="12"/></svg>'

const pipelineSources = (p) => [...(p.input_sources || []), ...(p.output_sources || [])]

export function generateGraphvizDot() {
    if (!state.currentConfig) return ''

    const edgeColor = '#555'
    // upstream_pipelines links with no datasource behind them ("run after").
    const explicitEdgeColor = '#9e9e9e'

    const nodeColors = {
        pipeline: { fill: '#e3f2fd', border: '#1976d2' },
        datasource: { fill: '#f3e5f5', border: '#7b1fa2' },
        group: { fill: '#fff3e0', border: '#ff6b35' }
    }

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

    // focusedSet is captured against the grouped view; if a focused group has
    // since been expanded, let its members and their sources through.
    let focusFilter = state.focusedSet
    if (focusFilter && state.groupedView) {
        focusFilter = new Set(state.focusedSet)
        for (const p of state.currentConfig.pipelines || []) {
            if (p.group && state.expandedGroups.has(p.group) && state.focusedSet.has(p.group)) {
                focusFilter.add(p.name)
                pipelineSources(p).forEach((s) => focusFilter.add(s))
            }
        }
    }
    const inFocus = (name) => !focusFilter || focusFilter.has(name)

    pipelines = pipelines.filter((p) => inFocus(p.name))

    const allDataSources = new Map()
    datasources.forEach((ds) => {
        if (inFocus(ds.name)) allDataSources.set(ds.name, ds)
    })

    pipelines.forEach((pipeline) => {
        pipelineSources(pipeline).forEach((sourceName) => {
            if (!allDataSources.has(sourceName) && inFocus(sourceName)) {
                allDataSources.set(sourceName, { name: sourceName, type: 'auto-created' })
            }
        })
    })

    // Focused: drop datasources no remaining pipeline touches.
    if (focusFilter) {
        const referenced = new Set(pipelines.flatMap(pipelineSources))
        for (const name of allDataSources.keys()) {
            if (!referenced.has(name)) allDataSources.delete(name)
        }
    }

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

    // The Pipelines view draws no datasources; don't let them hold clusters open.
    if (state.pipelinesOnlyView) allDataSources.clear()

    allDataSources.forEach((ds) => {
        const cluster = ds.cluster || '_unclustered'
        if (!nodesByCluster.has(cluster)) {
            nodesByCluster.set(cluster, [])
        }
        nodesByCluster.get(cluster).push({ type: 'datasource', node: ds })
    })

    // Recurse into descendants: skipping a cluster whose nodes live only in
    // grandchildren leaves edges to undeclared nodes (drawn as bare circles).
    function clusterHasNodes(name) {
        return nodesByCluster.get(name)?.length > 0 || (clusterChildren.get(name) || []).some(clusterHasNodes)
    }

    const rootClusters = []
    clusterDefinitions.forEach((cluster, name) => {
        if (!clusterHierarchy.has(name) && clusterHasNodes(name)) {
            rootClusters.push(name)
        }
    })

    let colorIndex = 0

    function renderCluster(clusterName, depth = 0) {
        const cluster = clusterDefinitions.get(clusterName)
        if (!cluster) return ''

        if (!clusterHasNodes(clusterName)) return ''

        const nodesInCluster = nodesByCluster.get(clusterName) || []
        const children = clusterChildren.get(clusterName) || []

        const clusterColor = clusterColors[colorIndex % clusterColors.length]
        colorIndex++

        let result = `${'    '.repeat(depth + 1)}subgraph cluster_${clusterName.replace(/[^a-zA-Z0-9]/g, '_')} {
${'    '.repeat(depth + 2)}label="${escapeDot(clusterName)}";
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
                const scheduleDisplay = formatSchedule(pipeline.schedule)
                const label = isGroup
                    ? `<${escapeHtml(pipeline.name)}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>`
                    : scheduleDisplay
                      ? `<${escapeHtml(pipeline.name)}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${escapeHtml(scheduleDisplay)}</I></FONT>>`
                      : `"${escapeDot(pipeline.name)}"`
                const fillColor = isGroup ? nodeColors.group.fill : nodeColors.pipeline.fill
                const borderColor = isGroup ? nodeColors.group.border : nodeColors.pipeline.border
                const shape = isGroup ? 'box3d' : 'box'
                const style = isGroup ? 'filled' : 'filled,rounded'
                const penWidth = isGroup ? '2' : '1'
                result += `${'    '.repeat(depth + 2)}"${escapeDot(pipeline.name)}" [shape=${shape}, style="${style}",
${'    '.repeat(depth + 3)}fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
${'    '.repeat(depth + 3)}fontname="Arial",
${'    '.repeat(depth + 3)}label=${label}];
`
            } else if (item.type === 'datasource' && !state.pipelinesOnlyView) {
                const ds = item.node
                result += `${'    '.repeat(depth + 2)}"${escapeDot(ds.name)}" [shape=ellipse, style=filled,
${'    '.repeat(depth + 3)}fillcolor="${nodeColors.datasource.fill}", color="${nodeColors.datasource.border}",
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
            const scheduleDisplay = formatSchedule(pipeline.schedule)
            const label = isGroup
                ? `<${escapeHtml(pipeline.name)}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>`
                : scheduleDisplay
                  ? `<${escapeHtml(pipeline.name)}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${escapeHtml(scheduleDisplay)}</I></FONT>>`
                  : `"${escapeDot(pipeline.name)}"`
            const fillColor = isGroup ? nodeColors.group.fill : nodeColors.pipeline.fill
            const borderColor = isGroup ? nodeColors.group.border : nodeColors.pipeline.border
            const shape = isGroup ? 'box3d' : 'box'
            const style = isGroup ? 'filled' : 'filled,rounded'
            const penWidth = isGroup ? '2' : '1'
            dot += `    "${escapeDot(pipeline.name)}" [shape=${shape}, style="${style}",
        fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
        fontname="Arial",
        label=${label}];
`
        } else if (item.type === 'datasource' && !state.pipelinesOnlyView) {
            const ds = item.node
            dot += `    "${escapeDot(ds.name)}" [shape=ellipse, style=filled,
        fillcolor="${nodeColors.datasource.fill}", color="${nodeColors.datasource.border}",
        fontname="Arial", fontsize=10];
`
        }
    })

    // Edges come from the same model lineage walks, so what's lit always matches
    // what's drawn. Only emit edges between drawn nodes; otherwise Graphviz
    // auto-creates the node, resurrecting off-path datasources when focused.
    const drawn = new Set([...pipelines.map((p) => p.name), ...allDataSources.keys()])
    dot += '\n'
    getLineageModel().edges.forEach(({ from, to, kind }) => {
        if (!drawn.has(from) || !drawn.has(to)) return
        const style =
            kind === 'explicit'
                ? `color="${explicitEdgeColor}", style="dashed", arrowsize=0.7`
                : `color="${edgeColor}", arrowsize=0.8`
        dot += `    "${escapeDot(from)}" -> "${escapeDot(to)}" [${style}];\n`
    })

    const nodeCount = pipelines.length + allDataSources.size
    state.lastGraphNodeCount = nodeCount
    const layoutAttrs =
        nodeCount > BIG_GRAPH_THRESHOLD
            ? 'splines=polyline; nslimit=2; mclimit=1; ranksep=0.5; nodesep=0.25;'
            : 'splines=true;'
    dot += `\n    overlap=false; ${layoutAttrs}\n}`
    return dot
}

let graphTabListenerAdded = false

export function renderGraph() {
    if (!state.currentConfig) return

    const currentHash = getConfigHash(state.currentConfig)

    if (!graphTabListenerAdded) {
        const graphPane = document.getElementById('graph-pane')
        if (graphPane) {
            const observer = new MutationObserver(function () {
                if (graphPane.classList.contains('active')) {
                    const newHash = getConfigHash(state.currentConfig)
                    if (!state.graphviz) {
                        setTimeout(initializeGraph, 100)
                        state.lastRenderedConfigHash = newHash
                    } else if (newHash !== state.lastRenderedConfigHash) {
                        updateGraph()
                        state.lastRenderedConfigHash = newHash
                    }
                }
            })
            observer.observe(graphPane, { attributes: true, attributeFilter: ['class'] })
        }
        graphTabListenerAdded = true
    }

    if (document.getElementById('graph-pane')?.classList.contains('active')) {
        if (!state.graphviz) {
            setTimeout(initializeGraph, 100)
        } else {
            resetRenderState()
            updateGraph()
        }
        state.lastRenderedConfigHash = currentHash
    }
}

export function initializeGraph() {
    try {
        resetRenderState()
        const el = document.getElementById('graph')
        el.innerHTML = ''
        createGraphviz(el)
        updateGraph()
    } catch (error) {
        console.error('Graphviz initialization error:', error)
        document.getElementById('graph').innerHTML = `
            <div style="padding: 1rem; color: var(--text-muted);">
                <strong>Graph rendering issue detected.</strong><br>
                Please check the Raw DOT tab to see the generated code.<br>
                <span style="font-size: 11px;">Error: ${escapeHtml(error.message)}</span>
            </div>
        `
    }
}

let isRendering = false
let pendingUpdate = false
// Sticky across coalesced updates: downgrading a queued instant render to a
// tween lets it be interrupted mid-morph, leaving unfilled paths behind.
let pendingInstant = false
let renderTimeout = null
// Set once the cached-SVG path has replaced #graph's DOM behind d3-graphviz.
let graphvizDirty = false

function resetRenderState() {
    if (renderTimeout) clearTimeout(renderTimeout)
    isRendering = false
    pendingUpdate = false
    pendingInstant = false
}

function flushPendingUpdate() {
    if (!pendingUpdate) return false
    const instant = pendingInstant
    pendingUpdate = false
    pendingInstant = false
    updateGraph(instant)
    return true
}

function createGraphviz(el) {
    const w = el.clientWidth || el.parentElement.clientWidth || 800
    const h = el.clientHeight || el.parentElement.clientHeight || 600
    // Morphing samples every edge path once per pixel by default, which takes
    // tens of seconds on a few hundred long edges; a fixed 20 points per path
    // looks the same at 150ms.
    state.graphviz = d3.select(el).graphviz().width(w).height(h).fit(true).zoom(true).tweenPrecision('5%')
    // The default [0.1, 10] can't zoom in far enough to read labels on huge graphs.
    state.graphviz.zoomScaleExtent?.(ZOOM_EXTENT)
}

// d3-graphviz caches its renderer on the element (`__graphviz__`) with data
// bindings and zoom tied to the old SVG; after a cached-SVG swap, start fresh.
function recreateGraphviz() {
    const el = document.getElementById('graph')
    el.innerHTML = ''
    delete el.__graphviz__
    createGraphviz(el)
}

function setGraphSpinner(show) {
    document.getElementById('graph-spinner')?.classList.toggle('show', show)
}

// Shared tail of the layout path and the cached-SVG restore path.
function finishRender(restored) {
    setupGraphInteractivity()
    if (!restored) setupLevelOfDetail()

    applyCriticalPathHighlighting()
    if (state.showCostLabels) showCostLabels()
    syncGraphToolbar()

    // Fit here rather than on a timer, otherwise d3-graphviz keeps the previous
    // transform and focused subgraphs land off-center.
    if (state.pendingFocusReset) {
        state.pendingFocusReset = false
        if (!restored) state.graphviz?.resetZoom()
    }
    // The SVG is new, so the selection's classes need re-applying.
    if (state.selectedNode) {
        refreshSelection()
        renderDetailsLineage()
        revealLineage()
    } else {
        selectNodeFromHash()
    }

    if (!flushPendingUpdate()) setTimeout(precomputeAdjacentStates, 100)
}

// graphviz's zoom is bound to the SVG it rendered, so a restored SVG needs its
// own. The cache is captured at fit, so identity == fitted.
let restoreZoom = null
function attachRestoreZoom() {
    const svg = d3.select('#graph').select('svg')
    if (svg.empty()) return
    const g = svg.select('g')
    const base = g.attr('transform') || ''

    const zoom = (restoreZoom = d3
        .zoom()
        .scaleExtent(ZOOM_EXTENT)
        .on('zoom', ({ transform: t }) => {
            g.attr('transform', `translate(${t.x},${t.y}) scale(${t.k}) ${base}`)
            applyLodClass()
        }))

    svg.call(zoom).on('dblclick.zoom', null).call(zoom.transform, d3.zoomIdentity)
    applyLodClass()
}

export function updateGraph(instant = false) {
    if (!state.graphviz) return

    if (isRendering) {
        pendingUpdate = true
        if (instant) pendingInstant = true
        return
    }

    const viewKey = getViewStateKey()
    let cached = state.viewStateCache.get(viewKey)
    if (!cached?.dot) {
        const fresh = { dot: generateGraphvizDot(), nodeCount: state.lastGraphNodeCount || 0 }
        if (cached) {
            Object.assign(cached, fresh)
        } else {
            cached = fresh
            addToViewCache(viewKey, cached)
        }
    }
    const nodeCount = cached.nodeCount || 0
    const big = nodeCount > BIG_GRAPH_THRESHOLD

    // Cached layout: swap the SVG in instead of re-running Graphviz. Deferred a
    // frame so the spinner paints before the synchronous innerHTML swap.
    if (cached.svg) {
        isRendering = true
        setGraphSpinner(true)
        if (renderTimeout) clearTimeout(renderTimeout)
        requestAnimationFrame(() => {
            presetLodForPaint(nodeCount)
            document.getElementById('graph').innerHTML = cached.svg
            graphvizDirty = true
            attachRestoreZoom()
            setGraphSpinner(false)
            isRendering = false
            finishRender(true)
        })
        return
    }

    if (graphvizDirty) {
        recreateGraphviz()
        graphvizDirty = false
    }

    // Morph transitions are too slow on big graphs, and the first render has
    // nothing to morph from.
    const instantRender = instant || big || !document.querySelector('#graph svg')

    isRendering = true
    const container = d3.select('#graph')
    if (!instantRender) container.style('opacity', 0.6)
    if (big || state.pendingFocusReset) setGraphSpinner(true)

    if (renderTimeout) clearTimeout(renderTimeout)
    renderTimeout = setTimeout(() => {
        if (!isRendering) return
        container.style('opacity', 1)
        setGraphSpinner(false)
        isRendering = false
        flushPendingUpdate()
    }, 3000)

    presetLodForPaint(nodeCount)

    const gv = instantRender ? state.graphviz : state.graphviz.transition(() => d3.transition().duration(150))
    // Bind 'end' before rendering: once wasm is warm an instant render can finish
    // synchronously, which would fire the previous render's handler and cache
    // this SVG under the previous view's key.
    gv.on('end', () => {
        if (renderTimeout) clearTimeout(renderTimeout)
        container.style('opacity', 1)
        isRendering = false
        // Captured before selection/critical-path classes are applied.
        if (big) cached.svg = document.getElementById('graph').innerHTML
        setGraphSpinner(false)
        finishRender(false)
    }).renderDot(cached.dot)
}

// Real on-screen scale (fit transform x zoom), so a huge graph fits label-free
// while a small graph that fits large keeps its labels.
function currentGraphScale() {
    const ctm = document.querySelector('#graph svg g')?.getScreenCTM?.()
    return ctm ? Math.abs(ctm.a) || 1 : 1
}

function applyLodClass() {
    const graphEl = document.getElementById('graph')
    if (!graphEl) return
    const s = currentGraphScale()
    graphEl.classList.toggle('lod-far', s < 0.32)
    graphEl.classList.toggle('lod-veryfar', s < 0.18)
}

// Set before paint so big graphs never draw labels only to hide them;
// applyLodClass refines once rendered.
function presetLodForPaint(nodeCount) {
    const graphEl = document.getElementById('graph')
    if (!graphEl) return
    const big = nodeCount > BIG_GRAPH_THRESHOLD
    graphEl.classList.toggle('lod-far', big)
    graphEl.classList.toggle('lod-veryfar', big)
}

function setupLevelOfDetail() {
    const zoom = state.graphviz?.zoomBehavior?.()
    if (!zoom || !state.graphviz.zoomSelection?.()) return
    zoom.on('zoom.lod', applyLodClass)
    applyLodClass()
}

function applyCriticalPathHighlighting() {
    if (!state.analysisMode) return

    let pathNodes, pathEdges, nodeClass, edgeClass

    if (state.analysisMode === 'critical-path') {
        pathNodes = getCriticalPathNodes()
        pathEdges = getCriticalPathEdges()
        nodeClass = 'node-critical-path'
        edgeClass = 'edge-critical-path'
    } else if (state.analysisMode === 'costliest-path') {
        pathNodes = getCostliestPathNodes()
        pathEdges = getCostliestPathEdges()
        nodeClass = 'node-costliest-path'
        edgeClass = 'edge-costliest-path'
    } else {
        return
    }

    if (pathNodes.size === 0) return

    d3.select('#graph')
        .selectAll('.node')
        .each(function () {
            const node = d3.select(this)
            const title = node.select('title').text()
            if (pathNodes.has(title)) {
                node.classed(nodeClass, true)
            } else {
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
                const edgeKey = `${match[1]}|${match[2]}`
                if (pathEdges.has(edgeKey)) {
                    edge.classed(edgeClass, true)
                } else {
                    edge.classed('edge-dimmed', true)
                }
            }
        })
}

function precomputeAdjacentStates() {
    if (!state.currentConfig?.pipelines) return

    const groupNames = []
    state.currentConfig.pipelines.forEach((p) => {
        if (p.group && !groupNames.includes(p.group)) groupNames.push(p.group)
    })

    const currentExpanded = new Set(state.expandedGroups)

    const bulkStates = [new Set(), new Set(groupNames)]
    bulkStates.forEach((bulkExpanded) => {
        const bulkKey = getViewStateKey(bulkExpanded, '')
        if (!state.viewStateCache.has(bulkKey)) {
            const savedExpanded = state.expandedGroups
            state.expandedGroups = bulkExpanded
            const bulkDot = generateGraphvizDot()
            state.expandedGroups = savedExpanded
            addToViewCache(bulkKey, { dot: bulkDot })
        }
    })

    let index = 0
    function processNext() {
        if (index >= groupNames.length) return

        const groupName = groupNames[index++]
        const wasExpanded = currentExpanded.has(groupName)

        const altExpanded = new Set(currentExpanded)
        if (wasExpanded) {
            altExpanded.delete(groupName)
        } else {
            altExpanded.add(groupName)
        }

        const altKey = getViewStateKey(altExpanded, '')

        if (!state.viewStateCache.has(altKey)) {
            const savedExpanded = state.expandedGroups
            state.expandedGroups = altExpanded
            const altDot = generateGraphvizDot()
            state.expandedGroups = savedExpanded
            addToViewCache(altKey, { dot: altDot })
        }

        if (index < groupNames.length) {
            setTimeout(processNext, 10)
        }
    }

    processNext()
}

// On-screen node name -> its <g>, and each drawn edge with its endpoints.
// Rebuilt after every render.
let nodeEls = new Map()
let edgeEls = []

function indexRenderedGraph() {
    nodeEls = new Map()
    edgeEls = []
    d3.select('#graph')
        .selectAll('.node')
        .each(function () {
            nodeEls.set(d3.select(this).select('title').text(), this)
        })
    d3.select('#graph')
        .selectAll('.edge')
        .each(function () {
            const match = d3
                .select(this)
                .select('title')
                .text()
                .match(/^(.+?)(?:->|--)\s*(.+?)$/)
            if (match) edgeEls.push({ el: this, from: match[1], to: match[2] })
        })
}

const isGroupNode = (name) => getLineageModel().members.has(name)

export function setupGraphInteractivity() {
    indexRenderedGraph()

    const incident = new Map()
    edgeEls.forEach((e) => {
        for (const n of [e.from, e.to]) {
            if (!incident.has(n)) incident.set(n, [])
            incident.get(n).push(e.el)
        }
    })
    const setHover = (el, on) => {
        const nodeName = d3.select(el).select('title').text()
        el.classList.toggle('node-hover', on)
        ;(incident.get(nodeName) || []).forEach((e) => e.classList.toggle('edge-hover', on))
    }

    // Click selects in place; double-click focuses, or opens a group. Group-node
    // clicks are delayed so a double-click doesn't also select.
    let nodeClickTimer = null
    d3.select('#graph')
        .selectAll('.node')
        .style('cursor', 'pointer')
        .on('click', function (event) {
            event.stopPropagation()
            const nodeName = d3.select(this).select('title').text()
            if (!isGroupNode(nodeName)) return selectNode(nodeName)
            clearTimeout(nodeClickTimer)
            nodeClickTimer = setTimeout(() => selectNode(nodeName), 220)
        })
        .on('dblclick', function (event) {
            event.stopPropagation()
            clearTimeout(nodeClickTimer)
            const nodeName = d3.select(this).select('title').text()
            if (isGroupNode(nodeName)) toggleGroup(nodeName)
            else toggleFocus(nodeName)
        })
        // Hover just previews the node's own edges; details live in the panel.
        .on('mouseenter', function () {
            setHover(this, true)
        })
        .on('mouseleave', function () {
            setHover(this, false)
        })

    // Clicking empty canvas (not the end of a pan) clears the selection.
    d3.select('#graph').on('click', function (event) {
        if (event.target.closest('.node')) return
        if (Math.abs(event.clientX - bgClickDownX) > 4 || Math.abs(event.clientY - bgClickDownY) > 4) return
        clearSelection()
    })

    restoreBlastRadiusFromHash()
}

export function selectNodeFromHash() {
    const nodeName = getNodeFromHash()
    if (!nodeName || nodeName === state.selectedNode) return
    if (nodeEls.has(nodeName)) selectNode(nodeName)
    else if (state.currentConfig) showNodeDetails(nodeName)
}

export function restoreBlastRadiusFromHash() {
    const blastNode = getBlastFromHash()
    if (blastNode && state.currentConfig) {
        showBlastRadius(blastNode)
    }
}

export function selectNode(nodeName) {
    state.selectedNode = nodeName
    updateHashWithNode(nodeName)
    refreshSelection()
    showNodeDetails(nodeName)
    syncGraphToolbar()
    revealLineage()
}

const hopClass = (h) => `hop-${Math.min(h, 3)}`

// Off-lineage nodes are dimmed, or hidden in place. Big graphs default to
// hiding, since a sea of grey doesn't help there; an explicit choice sticks.
const hidingOthers = () => state.hideUnrelated ?? nodeEls.size > BIG_GRAPH_THRESHOLD

// Light the selection's lineage within the current reach, upstream and
// downstream told apart, fading with distance; dim or hide everything else.
function refreshSelection() {
    clearHighlights()
    const name = state.selectedNode
    if (name && nodeEls.has(name)) {
        const { up, down } = litLineage(name)
        const hide = hidingOthers()
        const offNode = hide ? 'node-hidden' : 'node-dimmed'
        const offEdge = hide ? 'edge-hidden' : 'edge-dimmed'
        for (const [n, el] of nodeEls) {
            if (n === name) el.classList.add('node-highlighted')
            else if (up.has(n)) el.classList.add('node-connected', 'lin-up', hopClass(up.get(n)))
            else if (down.has(n)) el.classList.add('node-connected', 'lin-down', hopClass(down.get(n)))
            else el.classList.add(offNode)
        }
        for (const { el, from, to } of edgeEls) {
            if (up.has(from) && (to === name || up.has(to))) {
                el.classList.add('edge-highlighted', 'lin-up', hopClass(up.get(from)))
            } else if (down.has(to) && (from === name || down.has(from))) {
                el.classList.add('edge-highlighted', 'lin-down', hopClass(down.get(to)))
            } else {
                el.classList.add(offEdge)
            }
        }
        if (hide) hideEmptyClusters()
    }
    syncLineageControl()
}

// Cluster boxes don't contain their nodes in the SVG, so match by position.
function hideEmptyClusters() {
    const centers = []
    for (const el of nodeEls.values()) {
        if (el.classList.contains('node-hidden')) continue
        const b = el.getBBox()
        centers.push([b.x + b.width / 2, b.y + b.height / 2])
    }
    d3.select('#graph')
        .selectAll('.cluster')
        .each(function () {
            const b = this.getBBox()
            const used = centers.some(([x, y]) => x >= b.x && x <= b.x + b.width && y >= b.y && y <= b.y + b.height)
            if (!used) this.classList.add('cluster-hidden')
        })
}

// When hiding, the remaining lineage can be scattered across a huge canvas:
// bring it into view if part of it is off screen or too small to read.
function revealLineage() {
    if (!state.selectedNode || !hidingOthers()) return
    const rects = [...document.querySelectorAll('#graph .node-highlighted, #graph .node-connected')].map((el) =>
        el.getBoundingClientRect()
    )
    if (!rects.length) return
    const box = {
        left: Math.min(...rects.map((r) => r.left)),
        top: Math.min(...rects.map((r) => r.top)),
        right: Math.max(...rects.map((r) => r.right)),
        bottom: Math.max(...rects.map((r) => r.bottom))
    }
    moveView(box, { onlyIfHidden: true, fit: true })
}

export function toggleHideUnrelated() {
    if (state.selectedNode) setHideUnrelated(!hidingOthers())
}

export function setHideUnrelated(hide) {
    state.hideUnrelated = hide
    refreshSelection()
    revealLineage()
}

// The floating up/down reach slider; shown while a drawn node is selected.
function syncLineageControl() {
    const name = state.selectedNode
    if (!name || !nodeEls.has(name)) return updateLineageControl(null)
    const { up, down, maxUp, maxDown } = litLineage(name)
    updateLineageControl({
        name,
        maxUp,
        maxDown,
        reachUp: state.reachUp,
        reachDown: state.reachDown,
        upCount: up.size,
        downCount: down.size,
        focused: state.focusedNode === name,
        hiding: hidingOthers(),
        onReach: setLineageReach,
        onFocus: () => toggleFocus(name),
        onHide: setHideUnrelated
    })
}

let refocusTimer = null
let revealTimer = null
export function setLineageReach(dir, value) {
    if (dir === 'up') state.reachUp = value
    else state.reachDown = value
    refreshSelection()
    renderDetailsLineage()
    // A focused view shows exactly the slider's range; re-cut it once dragging settles.
    if (state.focusedNode && state.focusedNode === state.selectedNode) {
        clearTimeout(refocusTimer)
        refocusTimer = setTimeout(() => state.focusedNode && focusNode(state.focusedNode), 350)
    } else {
        clearTimeout(revealTimer)
        revealTimer = setTimeout(revealLineage, 350)
    }
}

// The node the details panel is showing; not always the selection (e.g. a
// group that was just expanded, or a search hit that isn't drawn).
let detailsNode = null

export function showNodeDetails(nodeName) {
    detailsNode = nodeName
    const col = document.getElementById('details-col')
    const content = document.getElementById('details-content')

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
        col.classList.remove('visible')
        return
    }

    const titleEl = document.getElementById('details-title')
    if (titleEl) titleEl.textContent = nodeData.name

    const nodeTypeBadge =
        nodeType === 'Pipeline' ? 'badge-pipeline' : nodeType === 'Data Source' ? 'badge-source' : 'badge-cluster'

    html = `<div class="detail-actions">`
    html += `<span class="badge ${nodeTypeBadge}">${nodeType}</span>`
    if (nodeType === 'Pipeline Group') {
        const isExpanded = state.expandedGroups.has(nodeName)
        html += `<button class="graph-ctrl-btn" onclick="toggleGroup(${jsArg(nodeName)})">${isExpanded ? 'Collapse' : 'Expand'}</button>`
    }
    if (nodeType === 'Pipeline' && nodeData.group && state.expandedGroups.has(nodeData.group)) {
        html += `<button class="graph-ctrl-btn" onclick="toggleGroup(${jsArg(nodeData.group)})">Collapse group</button>`
    }
    html += `<button class="graph-ctrl-btn danger" onclick="showBlastRadius(${jsArg(nodeName)})">Blast radius</button>`
    html += `</div>`

    if (nodeData.description) {
        html += `<div class="detail-description">${escapeHtml(nodeData.description)}</div>`
    }

    // Filled by renderDetailsLineage, which re-runs as the reach slider moves.
    html += `<div class="detail-section" id="details-lineage"></div>`

    const props = []
    if (nodeData.schedule) props.push(['Schedule', escapeHtml(formatSchedule(nodeData.schedule))])
    if (nodeType === 'Pipeline' || nodeType === 'Pipeline Group') {
        if (nodeData.duration !== undefined) props.push(['Duration', `${escapeHtml(nodeData.duration)}m`])
        if (nodeData.cost !== undefined) props.push(['Cost', `$${nodeData.cost.toFixed(2)}`])
    }
    if (nodeData.owners && nodeData.owners.length > 0) {
        props.push(['Owners', escapeHtml(nodeData.owners.join(', '))])
    } else if (nodeData.owner) {
        props.push(['Owner', escapeHtml(nodeData.owner)])
    }
    if (nodeData.users && nodeData.users.length > 0) {
        props.push(['Users', escapeHtml(nodeData.users.join(', '))])
    }
    if (nodeData.cluster)
        props.push(['Cluster', `<span class="badge badge-cluster">${escapeHtml(nodeData.cluster)}</span>`])
    if (nodeData.type) {
        const typeLower = (nodeData.type || '').toLowerCase()
        const typeBadge = ['snowflake', 'postgres', 'database'].includes(typeLower)
            ? `badge-${typeLower}`
            : ['s3', 'delta', 'parquet'].includes(typeLower)
              ? `badge-${typeLower}`
              : ['kafka', 'api'].includes(typeLower)
                ? `badge-${typeLower}`
                : 'badge-type'
        props.push(['Type', `<span class="badge ${typeBadge}">${escapeHtml(nodeData.type)}</span>`])
    }
    if (props.length) {
        html += `<div class="detail-section"><table class="detail-props">`
        props.forEach(([k, v]) => {
            html += `<tr><td class="detail-prop-key">${k}</td><td class="detail-prop-val">${v}</td></tr>`
        })
        html += `</table></div>`
    }

    if (nodeData.tags?.length) {
        html += `<div class="detail-section"><div class="detail-value">${nodeData.tags.map((t) => `<span class="badge badge-tag">${escapeHtml(t)}</span>`).join(' ')}</div></div>`
    }

    if (nodeData.links && Object.keys(nodeData.links).length) {
        html += `<div class="detail-section"><div class="detail-links">`
        Object.entries(nodeData.links).forEach(([name, url]) => {
            html += `<a href="${escapeHtml(safeUrl(url))}" target="_blank" class="detail-link-chip">${escapeHtml(name)}${LINK_ICON}</a>`
        })
        html += `</div></div>`
    }

    const hasConnectionDetails =
        nodeData._members?.length ||
        nodeData.input_sources?.length ||
        nodeData.output_sources?.length ||
        nodeData.upstream_pipelines?.length ||
        (nodeData.metadata && Object.keys(nodeData.metadata).length)

    if (hasConnectionDetails) {
        html += `<div class="detail-section detail-drawer" data-open="false">`
        html += `<div class="detail-drawer-toggle"><span class="detail-drawer-arrow">&#9656;</span> Connections &amp; details</div>`
        html += `<div class="detail-drawer-body">`

        if (nodeData._members?.length) {
            html += `<div class="detail-subsection"><div class="detail-label">Pipelines in this group <span class="detail-count">${nodeData._members.length}</span></div>`
            html += `<div class="detail-value">${nodeData._members.map((s) => `<span class="badge badge-pipeline">${escapeHtml(s)}</span>`).join(' ')}</div></div>`
        }

        if (nodeData.input_sources?.length) {
            html += `<div class="detail-subsection"><div class="detail-label">Reads from <span class="detail-count">${nodeData.input_sources.length}</span></div>`
            html += `<div class="detail-value">${nodeData.input_sources.map((s) => `<span class="badge badge-input">${escapeHtml(s)}</span>`).join(' ')}</div></div>`
        }
        if (nodeData.output_sources?.length) {
            html += `<div class="detail-subsection"><div class="detail-label">Writes to <span class="detail-count">${nodeData.output_sources.length}</span></div>`
            html += `<div class="detail-value">${nodeData.output_sources.map((s) => `<span class="badge badge-output">${escapeHtml(s)}</span>`).join(' ')}</div></div>`
        }
        if (nodeData.upstream_pipelines?.length) {
            html += `<div class="detail-subsection"><div class="detail-label">Depends on <span class="detail-count">${nodeData.upstream_pipelines.length}</span></div>`
            html += `<div class="detail-value">${nodeData.upstream_pipelines.map((s) => `<span class="badge badge-pipeline">${escapeHtml(s)}</span>`).join(' ')}</div></div>`
        }

        if (nodeData.metadata && Object.keys(nodeData.metadata).length) {
            html += `<div class="detail-subsection"><div class="detail-label">Metadata</div>`
            html += `<table class="detail-props">`
            Object.entries(nodeData.metadata).forEach(([key, value]) => {
                html += `<tr><td class="detail-prop-key">${escapeHtml(key)}</td><td class="detail-prop-val">${escapeHtml(value)}</td></tr>`
            })
            html += `</table></div>`
        }

        html += `</div></div>`
    }

    col.classList.add('visible')
    content.innerHTML = html
    renderDetailsLineage()

    content.querySelectorAll('.detail-drawer-toggle').forEach((toggle) => {
        toggle.addEventListener('click', function () {
            const drawer = this.closest('.detail-drawer')
            const isOpen = drawer.getAttribute('data-open') === 'true'
            drawer.setAttribute('data-open', isOpen ? 'false' : 'true')
        })
    })
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
    return { name, type: getLineageModel().isPipeline(name) ? 'group' : 'datasource' }
}

let lineageJsonView = false

// The panel's lineage list: what the slider lights, grouped by hop.
function renderDetailsLineage() {
    const box = document.getElementById('details-lineage')
    const nodeName = detailsNode
    if (!box || !nodeName || !state.currentConfig) return

    const model = getLineageModel()
    if (!model.nodes.has(nodeName)) {
        box.innerHTML = ''
        return
    }
    const { up, down, all } = litLineage(nodeName)

    const byHop = (m) => {
        const hops = new Map()
        ;[...m]
            .sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]))
            .forEach(([n, h]) => {
                if (!hops.has(h)) hops.set(h, [])
                hops.get(h).push(n)
            })
        return hops
    }

    const renderDir = (label, lit, full, dir) => {
        if (full.size === 0) return ''
        let out = `<div class="lineage-dir lineage-dir-${dir}"><div class="lineage-dir-label">${label} <span class="detail-count">${lit.size}</span></div>`
        for (const [h, names] of byHop(lit)) {
            out += `<div class="lineage-hop"><span class="lineage-hop-n" title="${h} hop${h > 1 ? 's' : ''} away">${h}</span><div class="lineage-hop-items">`
            names.forEach((n) => {
                const kind = model.isPipeline(n) ? 'badge-pipeline' : 'badge-source'
                const off = nodeEls.has(n) ? '' : ' lineage-chip-offscreen'
                out += `<button class="lineage-chip badge ${kind}${off}" data-node-name="${escapeHtml(n)}">${escapeHtml(n)}</button>`
            })
            out += `</div></div>`
        }
        const hidden = full.size - lit.size
        if (hidden > 0) {
            out += `<button class="lineage-more" data-dir="${dir}">+${hidden} further ${dir === 'up' ? 'upstream' : 'downstream'}</button>`
        }
        return out + `</div>`
    }

    if (all.up.size === 0 && all.down.size === 0) {
        box.innerHTML = `<div class="detail-label">Lineage</div><div class="lineage-empty">Nothing upstream or downstream.</div>`
        return
    }

    const edgesWithin = (keep) => model.edges.filter((e) => keep(e.from, e.to)).map((e) => ({ from: e.from, to: e.to }))
    const split = (m) => {
        const items = [...m].map(([n, depth]) => ({ ...getNodeMetadata(n), depth }))
        return {
            pipelines: items.filter((x) => model.isPipeline(x.name)),
            sources: items.filter((x) => !model.isPipeline(x.name))
        }
    }
    const lineageJson = {
        node: nodeName,
        ...getNodeMetadata(nodeName),
        upstream: { ...split(up), edges: edgesWithin((a, b) => up.has(a) && (b === nodeName || up.has(b))) },
        downstream: { ...split(down), edges: edgesWithin((a, b) => down.has(b) && (a === nodeName || down.has(a))) }
    }

    box.innerHTML = `
        <div class="lineage-head">
            <span class="detail-label">Lineage</span>
            <div class="lineage-view-toggle">
                <span class="lineage-toggle-label${lineageJsonView ? '' : ' active'}" data-view="tree">List</span>
                <div class="lineage-toggle-slider${lineageJsonView ? ' json-active' : ''}"></div>
                <span class="lineage-toggle-label${lineageJsonView ? ' active' : ''}" data-view="json">JSON</span>
            </div>
        </div>
        <div class="lineage-tree-view"${lineageJsonView ? ' style="display: none;"' : ''}>
            ${renderDir('Upstream', up, all.up, 'up')}
            ${renderDir('Downstream', down, all.down, 'down')}
        </div>
        <div class="lineage-json-view"${lineageJsonView ? '' : ' style="display: none;"'}>
            <pre class="lineage-json-pre">${escapeHtml(JSON.stringify(lineageJson, null, 2))}</pre>
        </div>`

    box.querySelector('.lineage-view-toggle').addEventListener('click', () => {
        lineageJsonView = !lineageJsonView
        renderDetailsLineage()
    })
    box.querySelectorAll('.lineage-chip').forEach((el) => {
        el.addEventListener('click', () => goToNode(el.dataset.nodeName))
    })
    box.querySelectorAll('.lineage-more').forEach((el) => {
        el.addEventListener('click', () => setLineageReach(el.dataset.dir, Infinity))
    })
}

// Select a node from the panel or search: in place if it's drawn, refocusing
// if it's outside the focused subgraph.
function goToNode(name, { zoomIn = false } = {}) {
    const el = nodeEls.get(name)
    if (el) {
        selectNode(name)
        if (!hidingOthers()) panToNode(el, { onlyIfHidden: !zoomIn, zoomIn })
    } else if (state.focusedNode && getLineageModel().nodes.has(name)) {
        focusNode(name)
    } else {
        showNodeDetails(name)
    }
}

function updateFocusBanner(nodeName) {
    const banner = document.getElementById('focus-banner')
    if (!banner) return
    banner.style.display = ''
    const label = banner.querySelector('.focus-banner-text')
    if (label) {
        label.textContent = `Focused: ${nodeName}`
        label.title = nodeName
    }
}

// Filter the graph to `nodeName` plus whatever the reach slider currently lights.
export function focusNode(nodeName) {
    const { up, down, maxUp, maxDown } = litLineage(nodeName)
    state.focusedNode = nodeName
    state.focusedSet = new Set([nodeName, ...up.keys(), ...down.keys()])
    state.focusReach = `${Math.min(state.reachUp, maxUp)}:${Math.min(state.reachDown, maxDown)}`
    state.selectedNode = nodeName
    updateHashWithNode(nodeName)
    updateFocusBanner(nodeName)
    showNodeDetails(nodeName)

    state.pendingFocusReset = true
    updateGraph(true)
}

export function toggleFocus(nodeName = state.selectedNode) {
    if (!nodeName) return
    if (state.focusedNode === nodeName) unfocusNode()
    else focusNode(nodeName)
}

// Back to the full graph, keeping the selection lit.
export function unfocusNode() {
    clearTimeout(refocusTimer)
    clearFocusState()
    state.pendingFocusReset = true
    updateGraph(true)
}

// Esc: leave focus first, then drop the selection.
export function stepBack() {
    if (state.focusedNode) unfocusNode()
    else if (state.selectedNode) clearSelection()
}

// Reset focus without re-rendering, for callers that re-render anyway.
function clearFocusState() {
    state.focusedNode = null
    state.focusedSet = null
    state.focusReach = null
    const banner = document.getElementById('focus-banner')
    if (banner) banner.style.display = 'none'
}

export function clearSelection() {
    state.selectedNode = null
    clearHighlights()
    applyCriticalPathHighlighting()
    updateHashWithNode(null)
    syncGraphToolbar()
    updateLineageControl(null)
    const col = document.getElementById('details-col')
    if (col) {
        col.classList.remove('visible')
    }
}

export function clearHighlights() {
    d3.select('#graph')
        .selectAll('.node')
        .classed(
            'node-highlighted node-connected node-dimmed node-hidden node-critical-path node-costliest-path lin-up lin-down hop-1 hop-2 hop-3',
            false
        )
    d3.select('#graph')
        .selectAll('.edge')
        .classed(
            'edge-highlighted edge-dimmed edge-hidden edge-critical-path edge-costliest-path lin-up lin-down hop-1 hop-2 hop-3',
            false
        )
    d3.select('#graph').selectAll('.cluster').classed('cluster-hidden', false)
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
            if (lastMatchIdx === i - 1) score += 0.5
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
            escapeHtml(text.substring(0, idx)) +
            '<span class="result-match">' +
            escapeHtml(text.substring(idx, idx + query.length)) +
            '</span>' +
            escapeHtml(text.substring(idx + query.length))
        )
    }

    let result = ''
    let queryIdx = 0
    for (let i = 0; i < text.length; i++) {
        if (queryIdx < query.length && text[i].toLowerCase() === lowerQuery[queryIdx]) {
            result += '<span class="result-match">' + escapeHtml(text[i]) + '</span>'
            queryIdx++
        } else {
            result += escapeHtml(text[i])
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
        dropdown.innerHTML = '<div class="search-result-item no-match">No matches found</div>'
        dropdown.classList.add('show')
        return
    }

    dropdown.innerHTML = topResults
        .map(
            (r, i) => `
        <div class="search-result-item${i === 0 ? ' selected' : ''}" data-name="${escapeHtml(r.name)}" onclick="selectSearchResult(${jsArg(r.name)})">
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

    // A pipeline inside a collapsed group is focused via its group node.
    let target = nodeName
    if (state.groupedView) {
        const p = state.currentConfig?.pipelines?.find((pl) => pl.name === nodeName)
        if (p?.group && !state.expandedGroups.has(p.group)) target = p.group
    }

    goToNode(nodeEls.has(target) || state.focusedNode ? target : nodeName, { zoomIn: true })
}

document.addEventListener('click', function (e) {
    const dropdown = document.getElementById('graph-search-results')
    const searchInput = document.getElementById('graph-search')
    if (dropdown && !dropdown.contains(e.target) && e.target !== searchInput) {
        dropdown.classList.remove('show')
    }
})

// Where the last press landed, to tell a background click from the end of a
// pan. Capture phase because graphviz's zoom stops mousedown propagation.
let bgClickDownX = 0
let bgClickDownY = 0
document.addEventListener(
    'mousedown',
    function (e) {
        bgClickDownX = e.clientX
        bgClickDownY = e.clientY
    },
    true
)

const NAV_DURATION = 400
// On-screen scale below which Recenter also zooms in, so the node is legible.
const RECENTER_MIN_SCALE = 0.6

// The live zoom: graphviz's own, or the one attached to a restored cached SVG.
function activeZoom() {
    const svg = d3.select('#graph').select('svg')
    const behavior = graphvizDirty ? restoreZoom : state.graphviz?.zoomBehavior?.()
    return svg.empty() || !behavior ? null : { svg, behavior }
}

// Animate back to the fitted view without touching focus or selection.
export function resetGraph() {
    const z = activeZoom()
    if (!z) return
    const t = d3.transition().duration(NAV_DURATION)
    if (graphvizDirty) z.svg.transition(t).call(z.behavior.transform, d3.zoomIdentity)
    else state.graphviz.resetZoom(t)
}

// Pan the selected node to the middle of the canvas, zooming in if it's too small
// to read. With nothing selected, re-fit the whole graph.
export function recenterSelection() {
    const el = state.selectedNode && nodeEls.get(state.selectedNode)
    if (!el) return resetGraph()
    panToNode(el, { zoomIn: true })
}

// Centre `el` in the visible canvas. `onlyIfHidden` leaves the view alone when
// the node is already on screen; `zoomIn` also zooms if it's too small to read.
function panToNode(el, { onlyIfHidden = false, zoomIn = false } = {}) {
    moveView(el.getBoundingClientRect(), { onlyIfHidden, zoomIn })
}

// Bring a screen-space box to the middle of the visible canvas. `zoomIn` zooms
// up to legible if it's too small; `fit` scales so the whole box fits (never
// past 1:1). `onlyIfHidden` skips boxes already on screen and legible.
function moveView(box, { onlyIfHidden = false, zoomIn = false, fit = false } = {}) {
    const z = activeZoom()
    if (!z) return

    // Zoom transforms live in the SVG's user space; convert screen px through its CTM.
    const svgNode = z.svg.node()
    const ctm = svgNode.getScreenCTM()
    if (!ctm) return
    const view = svgNode.getBoundingClientRect()
    // The details drawer and the reach slider overlay the canvas; aim for
    // what's left. Measured from layout, not the drawer's (maybe sliding) box.
    const drawer = document.getElementById('details-col')
    const right = drawer?.classList.contains('visible')
        ? Math.min(view.right, drawer.offsetParent.getBoundingClientRect().right - drawer.offsetWidth)
        : view.right
    const ctl = document.getElementById('lineage-ctl')
    const bottom = ctl && !ctl.hidden ? view.bottom - (ctl.offsetHeight + 24) : view.bottom
    const scale = currentGraphScale()
    const tooSmall = (zoomIn || fit) && scale < RECENTER_MIN_SCALE
    const onScreen = box.left >= view.left && box.right <= right && box.top >= view.top && box.bottom <= bottom
    if (onlyIfHidden && onScreen && !tooSmall) return

    const cx = (view.left + right) / 2
    const cy = (view.top + bottom) / 2
    const t = d3.zoomTransform(svgNode)

    let x = t.x + (cx - (box.left + box.right) / 2) / ctm.a
    let y = t.y + (cy - (box.top + box.bottom) / 2) / ctm.d
    let k = t.k
    let f = 1
    if (fit) {
        const pad = 40
        const w = Math.max(1, box.right - box.left)
        const h = Math.max(1, box.bottom - box.top)
        f = Math.min((right - view.left - 2 * pad) / w, (bottom - view.top - 2 * pad) / h, Math.max(1, 1 / scale))
    } else if (tooSmall) {
        f = 1 / scale
    }
    f = Math.min(f, ZOOM_EXTENT[1] / k)
    if (f !== 1) {
        // Scale about the canvas centre, where the box now sits.
        const ux = (cx - ctm.e) / ctm.a
        const uy = (cy - ctm.f) / ctm.d
        x = ux - f * (ux - x)
        y = uy - f * (uy - y)
        k *= f
    }

    z.svg.transition().duration(NAV_DURATION).call(z.behavior.transform, d3.zoomIdentity.translate(x, y).scale(k))
}

// Back to the whole graph: no focus, no selection, fitted.
export function showAll() {
    clearSelection()
    if (state.focusedNode) return unfocusNode()
    resetGraph()
}

export function collapseAllGroups() {
    clearFocusState()
    state.expandedGroups.clear()
    updateGraph(true)
}

export function setGroupsExpanded(expand) {
    clearFocusState()
    state.expandedGroups = new Set(expand ? getAllGroupNames() : [])
    updateGraph(true)
}

// Reflect current state in the graph toolbar's segmented controls and nav buttons.
export function syncGraphToolbar() {
    const setSeg = (id, value) => {
        document.querySelectorAll(`#${id} .seg-btn`).forEach((b) => {
            const on = b.dataset.value === value
            b.classList.toggle('active', on)
            b.setAttribute('aria-pressed', on)
        })
    }

    const groups = getAllGroupNames()
    const groupsCtl = document.getElementById('groups-ctl')
    if (groupsCtl) groupsCtl.style.display = groups.length ? '' : 'none'
    const expanded = groups.filter((g) => state.expandedGroups.has(g)).length
    // Partly expanded (groups toggled one by one) lights neither segment.
    setSeg('groups-ctl', expanded === groups.length ? 'expanded' : expanded === 0 ? 'collapsed' : null)

    const recenter = document.getElementById('recenter-btn')
    if (recenter) {
        recenter.title = state.selectedNode
            ? `Center the view on ${state.selectedNode}`
            : 'Center and fit the graph (keeps focus and selection)'
    }
}

function getAllGroupNames() {
    if (!state.currentConfig?.pipelines) return []
    const groups = new Set()
    state.currentConfig.pipelines.forEach((p) => {
        if (p.group) groups.add(p.group)
    })
    return Array.from(groups)
}

// Lineage view (pipelines and datasources) vs the Pipelines view.
export function setDatasourcesVisible(show) {
    if (state.pipelinesOnlyView === !show) return
    state.pipelinesOnlyView = !show
    // Lineage differs between the two, so a focus cut in one doesn't carry over.
    clearFocusState()
    if (state.selectedNode && !getLineageModel().nodes.has(state.selectedNode)) clearSelection()
    state.pendingFocusReset = true
    updateGraph(true)
}

export function setAnalysisMode(mode) {
    state.analysisMode = mode || null
    clearCriticalPathHighlighting()
    applyCriticalPathHighlighting()
}

export function toggleAnalysisMode(mode, checkbox) {
    if (checkbox.checked) {
        setAnalysisMode(mode)
    } else {
        setAnalysisMode(null)
    }
}

export function toggleCostLabels(show) {
    state.showCostLabels = show
    if (show) {
        showCostLabels()
    } else {
        hideCostLabels()
    }
}

function showCostLabels() {
    const pipelines = state.currentConfig?.pipelines || []
    const pipelineMap = new Map()
    pipelines.forEach((p) => pipelineMap.set(p.name, p))

    d3.select('#graph')
        .selectAll('.node')
        .each(function () {
            const node = d3.select(this)
            const title = node.select('title').text()
            const pipeline = pipelineMap.get(title)

            if (pipeline?.cost !== undefined) {
                const bbox = this.getBBox()
                const cost = pipeline.cost
                const costText = cost >= 1 ? `$${cost.toFixed(0)}` : `$${cost.toFixed(2)}`

                node.append('text')
                    .attr('class', 'cost-label')
                    .attr('x', bbox.x + bbox.width / 2)
                    .attr('y', bbox.y - 4)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '10px')
                    .attr('font-weight', '600')
                    .attr('fill', '#2e7d32')
                    .text(costText)
            }
        })
}

function hideCostLabels() {
    d3.select('#graph').selectAll('.cost-label').remove()
}

function clearCriticalPathHighlighting() {
    d3.select('#graph').selectAll('.node').classed('node-critical-path node-costliest-path node-dimmed', false)
    d3.select('#graph').selectAll('.edge').classed('edge-critical-path edge-costliest-path edge-dimmed', false)
}

export function toggleGroup(groupName) {
    if (state.expandedGroups.has(groupName)) {
        state.expandedGroups.delete(groupName)
    } else {
        state.expandedGroups.add(groupName)
    }
    updateGraph(true)
    showNodeDetails(groupName)
}

export function showBlastRadius(nodeName) {
    const modal = document.getElementById('blast-radius-modal')
    const graphContainer = document.getElementById('blast-radius-graph')
    const summaryContainer = document.getElementById('blast-radius-summary')
    const nodeNameEl = document.getElementById('blast-radius-node-name')

    if (!modal || !graphContainer || !summaryContainer) return

    updateHashWithBlast(nodeName)

    graphContainer.innerHTML = ''
    blastRadiusGraphInstance = null

    nodeNameEl.textContent = nodeName

    const analysis = generateBlastRadiusAnalysis(nodeName)

    if (!analysis || analysis.downstream.length === 0) {
        summaryContainer.innerHTML = `
            <div style="text-align: center; padding: 2rem; color: var(--text-muted);">
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
        modal.classList.add('show')
        return
    }

    const isGroup = analysis.source_type === 'group'
    let summaryHtml = `
        <div class="lineage-view-toggle" style="margin-bottom: 0.75rem;">
            <span class="lineage-toggle-label active" data-view="tree">Summary</span>
            <div class="lineage-toggle-slider" id="blast-radius-toggle"></div>
            <span class="lineage-toggle-label" data-view="json">JSON</span>
        </div>
        <div id="blast-radius-tree-view">
    `

    if (isGroup && analysis.group_members) {
        summaryHtml += `
            <div class="mb-3">
                <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;">Group Members (${analysis.group_size})</div>
                <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
                    ${analysis.group_members.map((m) => `<span style="font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid #ff6b35;">▢ ${escapeHtml(m)}</span>`).join('')}
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

    Object.entries(analysis.by_depth).forEach(([depth, nodes]) => {
        const depthColors = ['#c98b8b', '#d4a574', '#c4c474', '#7cb47c', '#6b9dc4', '#a88bc4']
        const color = depthColors[Math.min(parseInt(depth), depthColors.length - 1)]

        summaryHtml += `
            <div class="mb-2">
                <div style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;">
                    Depth ${escapeHtml(depth)} <span style="color: ${color};">●</span>
                </div>
                <div style="display: flex; flex-wrap: wrap; gap: 0.25rem;">
        `
        nodes.forEach((node) => {
            const icon = node.type === 'pipeline' ? '▢' : '○'
            summaryHtml += `<span style="font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid ${color};">${icon} ${escapeHtml(node.name)}</span>`
        })
        summaryHtml += `</div></div>`
    })

    summaryHtml += `</div>`

    summaryHtml += `
        <div id="blast-radius-json-view" style="display: none;">
            <pre style="font-size: 0.7rem; max-height: 400px; overflow: auto; background: var(--bg-code); color: var(--text-primary); padding: 0.75rem; border-radius: 4px; margin: 0; border: 1px solid var(--border-color);">${escapeHtml(JSON.stringify(analysis, null, 2))}</pre>
        </div>
    `

    summaryContainer.innerHTML = summaryHtml

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

    modal.classList.add('show')

    setTimeout(() => {
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
                    .zoom(false)
                    .on('end', function () {
                        const svg = d3.select('#blast-radius-graph svg')
                        const g = svg.select('g')

                        const zoom = d3
                            .zoom()
                            .scaleExtent([0.1, 4])
                            .on('zoom', function (event) {
                                g.attr('transform', event.transform)
                            })

                        svg.call(zoom)

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
    }, 50)
}

export function hideBlastRadiusModal() {
    const modal = document.getElementById('blast-radius-modal')
    if (modal) {
        modal.classList.remove('show')
        clearBlastFromHash()
    }
}

export function toggleOverflowMenu(event) {
    event.stopPropagation()
    const menu = document.getElementById('graph-overflow-menu')
    menu.classList.toggle('show')
    updateOverflowMenuState()
}

export function updateOverflowMenuState() {
    const pipelinesOnlyBtn = document.getElementById('pipelines-only-btn')
    const cpMenuItem = document.getElementById('cp-menu-item')
    const costMenuItem = document.getElementById('cost-menu-item')

    if (pipelinesOnlyBtn) {
        pipelinesOnlyBtn.classList.toggle('active', state.pipelinesOnlyView)
    }
    if (cpMenuItem) {
        cpMenuItem.classList.toggle('active', state.analysisMode === 'critical-path')
    }
    if (costMenuItem) {
        costMenuItem.classList.toggle('active', state.showCostLabels)
    }
}

export function toggleAnalysisModeFromMenu(mode) {
    if (state.analysisMode === mode) {
        setAnalysisMode(null)
    } else {
        setAnalysisMode(mode)
    }
    updateOverflowMenuState()
}

export function toggleCostLabelsFromMenu() {
    toggleCostLabels(!state.showCostLabels)
    updateOverflowMenuState()
}

document.addEventListener('click', function (e) {
    const menu = document.getElementById('graph-overflow-menu')
    if (menu && !e.target.closest('.overflow-menu-container')) {
        menu.classList.remove('show')
    }
})
