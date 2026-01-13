import { state, getConfigHash } from './state.js'
export function generateAttributeDot() {
    if (!state.currentConfig) return ''

    const datasources = state.currentConfig.datasources || []
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const textColor = '#334155'
    const clusterBg = '#f1f5f9'
    const attrFill = '#ffffff'
    const attrBorder = '#94a3b8'

    const maxLen = 20
    function truncate(name) {
        if (name.length <= maxLen) return name
        return name.substring(0, maxLen - 1) + '…'
    }

    let dot = `digraph AttributeLineage {
    rankdir=LR
    bgcolor="transparent"
    compound=true
    nodesep=0.15
    node [fontname="Arial" fontsize="10"]
    edge [fontsize="9" color="#94a3b8" arrowsize="0.6"]

`

    const sourceRefs = new Set()
    function collectSourceRefs(attrs) {
        attrs.forEach((attr) => {
            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach((s) => sourceRefs.add(s))
            }
            if (attr.attributes) collectSourceRefs(attr.attributes)
        })
    }
    datasources.forEach((ds) => {
        if (ds.attributes) collectSourceRefs(ds.attributes)
    })

    function isSourceRef(dsName, attrPath) {
        const ref = `${dsName}::${attrPath.replace(/__/g, '::')}`
        return sourceRefs.has(ref)
    }

    const structAttrs = new Set()
    state.nestedClusterCount = 0
    function renderAttributes(attrs, dsName, dsId, prefix = '', depth = 0) {
        let result = ''
        attrs.forEach((attr) => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const attrId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const hasUpstream = attr.from ? true : false
            const hasDownstream = isSourceRef(dsName, attrPath)
            const hasLineage = hasUpstream || hasDownstream
            const hasChildren = attr.attributes && attr.attributes.length > 0

            if (hasChildren) {
                structAttrs.add(attrId)
                state.nestedClusterCount++
                const clusterId = `cluster_${attrId}`
                const nestedBg = hasLineage ? '#dde5ed' : depth === 0 ? '#e8eef4' : '#dce4ec'
                result += `        subgraph ${clusterId} {
                    label="${attr.name}"
                    labelloc="t"
                    style=filled
                    fillcolor="${nestedBg}"
                    fontcolor="${hasLineage ? '#7b1fa2' : textColor}"
                    color="${hasLineage ? '#7b1fa2' : attrBorder}"
                    fontname="Arial"
                    fontsize="9"
                    margin="8"
`
                result += `            "${attrId}" [label="" shape=point width=0 height=0 fixedsize=true style=invis];\n` // anchor
                result += renderAttributes(attr.attributes, dsName, dsId, attrPath, depth + 1)
                result += `                        }\n`
            } else {
                const fill = hasLineage ? '#e2e8f0' : attrFill
                result += `            "${attrId}" [label="${attr.name}" shape=box style="filled,rounded" fillcolor="${fill}" color="${attrBorder}" fontcolor="${textColor}" fontsize="9" height="0.3"];\n`
            }
        })
        return result
    }

    function collectLineage(attrs, dsId, prefix = '') {
        let edges = []
        attrs.forEach((attr) => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const targetId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const targetIsStruct = structAttrs.has(targetId)

            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach((source) => {
                    const parts = source.split('::')
                    if (parts.length >= 2) {
                        const sourceDs = parts[0].replace(/[^a-zA-Z0-9]/g, '_')
                        const sourcePath = parts
                            .slice(1)
                            .join('__')
                            .replace(/[^a-zA-Z0-9_]/g, '_')
                        const sourceId = `${sourceDs}__${sourcePath}`
                        const sourceIsStruct = structAttrs.has(sourceId)

                        let edgeAttrs = ['color="#7b1fa2"']

                        if (sourceIsStruct) {
                            edgeAttrs.push(`ltail="cluster_${sourceId}"`)
                        }
                        if (targetIsStruct) {
                            edgeAttrs.push(`lhead="cluster_${targetId}"`)
                        }

                        edges.push(`    "${sourceId}" -> "${targetId}" [${edgeAttrs.join(' ')}];\n`)
                    }
                })
            }

            if (attr.attributes && attr.attributes.length > 0) {
                edges = edges.concat(collectLineage(attr.attributes, dsId, attrPath))
            }
        })
        return edges
    }

    datasources.forEach((ds) => {
        if (!ds.attributes || ds.attributes.length === 0) return

        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        dot += `    subgraph cluster_${dsId} {
        label="${ds.name}"
        style=filled
        fillcolor="${clusterBg}"
        fontcolor="${textColor}"
        color="${attrBorder}"
        fontname="Arial"
        fontsize="11"

`
        dot += renderAttributes(ds.attributes, ds.name, dsId)
        dot += `    }\n\n`
    })

    datasources.forEach((ds) => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        const edges = collectLineage(ds.attributes, dsId)
        edges.forEach((edge) => {
            dot += edge
        })
    })

    dot += `}`
    return dot
}

export function renderAttributeGraph() {
    if (!state.currentConfig) return

    const dot = generateAttributeDot()
    if (!dot) return

    if (!state.attributeGraphviz) {
        document.getElementById('attribute-graph').innerHTML = ''
        state.attributeGraphviz = d3
            .select('#attribute-graph')
            .graphviz()
            .width(document.getElementById('attribute-graph').offsetWidth || 800)
            .height(500)
            .fit(true)
            .zoom(true)
    }

    state.attributeGraphviz.renderDot(dot).on('end', function () {
        d3.select('#attribute-graph')
            .selectAll('.node')
            .on('click', function (event) {
                event.stopPropagation()
                const title = d3.select(this).select('title').text()
                if (title && state.attributeLineageMap[title]) {
                    selectAttribute(title)
                }
            })

        d3.select('#attribute-graph')
            .selectAll('.cluster')
            .on('click', function (event) {
                event.stopPropagation()
                const title = d3.select(this).select('title').text()
                if (title && title.startsWith('cluster_')) {
                    const id = title.substring(8)
                    if (id.includes('__') && state.attributeLineageMap[id]) {
                        selectAttribute(id)
                    } else {
                        const ds = (state.currentConfig.datasources || []).find(
                            (d) => d.name.replace(/[^a-zA-Z0-9]/g, '_') === id
                        )
                        if (ds) showDatasourceInAttributePanel(ds)
                    }
                }
            })

        d3.select('#attribute-graph svg').on('click', function (event) {
            if (event.target.tagName === 'svg' || event.target.classList.contains('graph')) {
                clearAttributeSelection()
            }
        })
    })
}

export function fitAttributeGraph() {
    if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
}

export function resetAttributeGraph() {
    if (state.attributeGraphviz) state.attributeGraphviz.resetZoom()
}

export function buildAttributeLineageMap() {
    state.attributeLineageMap = {}
    if (!state.currentConfig) return

    const datasources = state.currentConfig.datasources || []

    function registerAttributes(attrs, dsName, dsId, prefix = '') {
        attrs.forEach((attr) => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const attrId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const fullName = prefix
                ? `${dsName}::${prefix.replace(/__/g, '::')}::${attr.name}`
                : `${dsName}::${attr.name}`

            state.attributeLineageMap[attrId] = {
                id: attrId,
                name: attr.name,
                datasource: dsName,
                fullName: fullName,
                upstream: [],
                downstream: []
            }

            if (attr.attributes && attr.attributes.length > 0) {
                registerAttributes(attr.attributes, dsName, dsId, attrPath)
            }
        })
    }

    function collectAttributeLineage(attrs, dsId, prefix = '') {
        attrs.forEach((attr) => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const targetId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`

            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach((source) => {
                    const parts = source.split('::')
                    if (parts.length >= 2) {
                        const sourceDs = parts[0].replace(/[^a-zA-Z0-9]/g, '_')
                        const sourcePath = parts
                            .slice(1)
                            .join('__')
                            .replace(/[^a-zA-Z0-9_]/g, '_')
                        const sourceId = `${sourceDs}__${sourcePath}`

                        if (state.attributeLineageMap[targetId]) {
                            state.attributeLineageMap[targetId].upstream.push(sourceId)
                        }
                        if (state.attributeLineageMap[sourceId]) {
                            state.attributeLineageMap[sourceId].downstream.push(targetId)
                        }
                    }
                })
            }

            if (attr.attributes && attr.attributes.length > 0) {
                collectAttributeLineage(attr.attributes, dsId, attrPath)
            }
        })
    }

    datasources.forEach((ds) => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        registerAttributes(ds.attributes, ds.name, dsId)
    })

    datasources.forEach((ds) => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        collectAttributeLineage(ds.attributes, dsId)
    })

    function computeFullChain(attrId, direction, visited = new Set(), depth = 1) {
        if (visited.has(attrId)) return []
        visited.add(attrId)
        const attr = state.attributeLineageMap[attrId]
        if (!attr) return []
        const next = direction === 'upstream' ? attr.upstream : attr.downstream
        let result = []
        next.forEach((nextId) => {
            result.push({ id: nextId, depth: depth })
            result.push(...computeFullChain(nextId, direction, visited, depth + 1))
        })
        return result
    }

    Object.keys(state.attributeLineageMap).forEach((attrId) => {
        state.attributeLineageMap[attrId].fullUpstream = computeFullChain(attrId, 'upstream', new Set())
        state.attributeLineageMap[attrId].fullDownstream = computeFullChain(attrId, 'downstream', new Set())
    })

    var dsDirectUpstream = {}
    var dsDirectDownstream = {}

    datasources.forEach(function (ds) {
        var id = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        dsDirectUpstream[id] = new Set()
        dsDirectDownstream[id] = new Set()
    })

    Object.values(state.attributeLineageMap).forEach(function (attr) {
        if (!attr || !attr.datasource) return
        var dsId = attr.datasource.replace(/[^a-zA-Z0-9]/g, '_')

        var upIds = attr.upstream || []
        for (var i = 0; i < upIds.length; i++) {
            var upDs = upIds[i].split('__')[0]
            if (upDs && upDs !== dsId && dsDirectUpstream[dsId]) {
                dsDirectUpstream[dsId].add(upDs)
            }
        }

        var downIds = attr.downstream || []
        for (var j = 0; j < downIds.length; j++) {
            var downDs = downIds[j].split('__')[0]
            if (downDs && downDs !== dsId && dsDirectDownstream[dsId]) {
                dsDirectDownstream[dsId].add(downDs)
            }
        }
    })

    function bfsProvenance(startId, getNeighbors) {
        var result = []
        var visited = new Set([startId])
        var queue = []
        var neighbors = getNeighbors(startId)
        neighbors.forEach(function (id) {
            queue.push({ id: id, depth: 1 })
            visited.add(id)
        })

        while (queue.length > 0) {
            var item = queue.shift()
            result.push(item)
            var next = getNeighbors(item.id)
            next.forEach(function (nextId) {
                if (!visited.has(nextId)) {
                    visited.add(nextId)
                    queue.push({ id: nextId, depth: item.depth + 1 })
                }
            })
        }

        result.sort(function (a, b) {
            return a.depth - b.depth
        })
        return result
    }

    state.datasourceLineageMap = {}
    datasources.forEach(function (ds) {
        var id = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        state.datasourceLineageMap[id] = {
            name: ds.name,
            upstream: bfsProvenance(id, function (x) {
                return dsDirectUpstream[x] || new Set()
            }),
            downstream: bfsProvenance(id, function (x) {
                return dsDirectDownstream[x] || new Set()
            })
        }
    })
}

export function getFullProvenance(attrId, direction, visited = new Set()) {
    if (visited.has(attrId)) return []
    visited.add(attrId)

    const attr = state.attributeLineageMap[attrId]
    if (!attr) return []

    const next = direction === 'upstream' ? attr.upstream : attr.downstream
    let result = []

    next.forEach((nextId) => {
        result.push(nextId)
        result.push(...getFullProvenance(nextId, direction, visited))
    })

    return result
}

export function selectAttribute(attrId) {
    state.selectedAttribute = attrId
    const attr = state.attributeLineageMap[attrId]
    if (!attr) return

    d3.select('#attribute-graph')
        .selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)
    d3.select('#attribute-graph')
        .selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)
    d3.select('#attribute-graph').selectAll('.edge').classed('edge-highlighted', false).classed('edge-dimmed', false)

    const upstream = attr.fullUpstream || []
    const downstream = attr.fullDownstream || []
    const allConnected = new Set([attrId, ...upstream.map((x) => x.id), ...downstream.map((x) => x.id)])

    d3.select('#attribute-graph')
        .selectAll('.node')
        .each(function () {
            const node = d3.select(this)
            const title = node.select('title').text()
            if (title === attrId) {
                node.classed('node-highlighted', true).classed('node-dimmed', false)
            } else if (allConnected.has(title)) {
                node.classed('node-connected', true).classed('node-dimmed', false)
            } else {
                node.classed('node-dimmed', true)
            }
        })

    d3.select('#attribute-graph')
        .selectAll('.cluster')
        .each(function () {
            const cluster = d3.select(this)
            const title = cluster.select('title').text()
            if (title && title.startsWith('cluster_')) {
                const clusterId = title.substring(8)
                if (clusterId === attrId) {
                    cluster.classed('cluster-highlighted', true).classed('cluster-dimmed', false)
                } else if (allConnected.has(clusterId)) {
                    cluster.classed('cluster-connected', true).classed('cluster-dimmed', false)
                } else {
                    cluster.classed('cluster-dimmed', true)
                }
            }
        })

    d3.select('#attribute-graph')
        .selectAll('.edge')
        .each(function () {
            const edge = d3.select(this)
            const title = edge.select('title').text()
            const [from, to] = title.split('->').map((s) => s.trim())
            if (allConnected.has(from) && allConnected.has(to)) {
                edge.classed('edge-highlighted', true).classed('edge-dimmed', false)
            } else {
                edge.classed('edge-dimmed', true)
            }
        })

    showAttributeDetails(attrId, upstream, downstream)
}

export function showAttributeDetails(attrId, upstream, downstream) {
    const attr = state.attributeLineageMap[attrId]
    if (!attr) return

    const col = document.getElementById('attribute-details-col')
    const graphCol = document.getElementById('attribute-graph-col')
    const content = document.getElementById('attribute-details-content')

    graphCol.classList.remove('col-md-12')
    graphCol.classList.add('col-md-8')
    col.style.display = 'block'

    setTimeout(() => {
        if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
    }, 50)

    let html = `<h5>${attr.name}</h5>`
    html += `<div class="detail-label">DATASOURCE</div>`
    html += `<div class="detail-value">${attr.datasource}</div>`

    const childPrefix = attrId + '__'
    const children = Object.keys(state.attributeLineageMap)
        .filter((id) => id.startsWith(childPrefix))
        .map((id) => {
            const child = state.attributeLineageMap[id]
            const relativePath = id.substring(childPrefix.length)
            const depth = (relativePath.match(/__/g) || []).length
            return { id, name: child.name, relativePath, depth }
        })
        .sort((a, b) => a.relativePath.localeCompare(b.relativePath))

    if (children.length > 0) {
        html += `<div class="detail-label">CONTAINS (${children.length})</div>`
        html += `<div class="detail-value">`
        children.forEach((child) => {
            const indent = child.depth * 12
            html += `<div class="lineage-link" data-attr-id="${child.id}" style="padding-left: ${indent}px;">${child.relativePath.replace(/__/g, '.')}</div>`
        })
        html += `</div>`
    }

    function dedupeAndSort(items) {
        const seen = new Map()
        items.forEach((x) => {
            if (!seen.has(x.id) || seen.get(x.id).depth > x.depth) {
                seen.set(x.id, x)
            }
        })
        return [...seen.values()].sort((a, b) => a.depth - b.depth)
    }

    const sortedUpstream = dedupeAndSort(upstream)
    const sortedDownstream = dedupeAndSort(downstream)

    function getDatasourceMetadata(dsName) {
        const datasource = state.currentConfig?.datasources?.find((ds) => ds.name === dsName)
        if (!datasource) return null
        const meta = {}
        if (datasource.description) meta.description = datasource.description
        if (datasource.type) meta.source_type = datasource.type
        if (datasource.cluster) meta.cluster = datasource.cluster
        if (datasource.owner) meta.owner = datasource.owner
        if (datasource.tags?.length) meta.tags = datasource.tags
        if (datasource.links && Object.keys(datasource.links).length) meta.links = datasource.links
        if (datasource.metadata && Object.keys(datasource.metadata).length) meta.metadata = datasource.metadata
        return meta
    }

    const allUpstreamIds = new Set(sortedUpstream.map((x) => x.id))
    const allDownstreamIds = new Set(sortedDownstream.map((x) => x.id))
    const upstreamEdges = []
    allUpstreamIds.forEach((id) => {
        const upAttr = state.attributeLineageMap[id]
        if (upAttr && upAttr.downstream) {
            upAttr.downstream.forEach((downId) => {
                if (allUpstreamIds.has(downId) || downId === attrId) {
                    upstreamEdges.push({ from: id, to: downId })
                }
            })
        }
    })

    const downstreamEdges = []
    if (attr.downstream) {
        attr.downstream.forEach((downId) => {
            if (allDownstreamIds.has(downId)) {
                downstreamEdges.push({ from: attrId, to: downId })
            }
        })
    }
    allDownstreamIds.forEach((id) => {
        const downAttr = state.attributeLineageMap[id]
        if (downAttr && downAttr.downstream) {
            downAttr.downstream.forEach((nextId) => {
                if (allDownstreamIds.has(nextId)) {
                    downstreamEdges.push({ from: id, to: nextId })
                }
            })
        }
    })

    const lineageJson = {
        attribute: attrId,
        name: attr.name,
        fullName: attr.fullName,
        datasource: attr.datasource,
        datasource_metadata: getDatasourceMetadata(attr.datasource),
        upstream: {
            attributes: sortedUpstream
                .map((x) => {
                    const upAttr = state.attributeLineageMap[x.id]
                    if (!upAttr) return null
                    return {
                        id: x.id,
                        depth: x.depth,
                        name: upAttr.name,
                        fullName: upAttr.fullName,
                        datasource: upAttr.datasource,
                        datasource_metadata: getDatasourceMetadata(upAttr.datasource)
                    }
                })
                .filter((x) => x),
            edges: upstreamEdges
        },
        downstream: {
            attributes: sortedDownstream
                .map((x) => {
                    const downAttr = state.attributeLineageMap[x.id]
                    if (!downAttr) return null
                    return {
                        id: x.id,
                        depth: x.depth,
                        name: downAttr.name,
                        fullName: downAttr.fullName,
                        datasource: downAttr.datasource,
                        datasource_metadata: getDatasourceMetadata(downAttr.datasource)
                    }
                })
                .filter((x) => x),
            edges: downstreamEdges
        },
        children: children.map((c) => ({
            id: c.id,
            name: c.name,
            relativePath: c.relativePath,
            depth: c.depth
        }))
    }

    const hasLineage = sortedUpstream.length > 0 || sortedDownstream.length > 0

    if (hasLineage) {
        html += `<div class="lineage-view-toggle">
            <span class="lineage-toggle-label active" data-view="tree">Tree</span>
            <div class="lineage-toggle-slider"></div>
            <span class="lineage-toggle-label" data-view="json">JSON</span>
        </div>`
    }

    html += `<div class="lineage-tree-view">`

    if (sortedUpstream.length > 0) {
        html += `<div class="detail-label">UPSTREAM (${sortedUpstream.length})</div>`
        html += `<div class="detail-value">`
        sortedUpstream.forEach((x) => {
            const upAttr = state.attributeLineageMap[x.id]
            if (upAttr) {
                const indent = (x.depth - 1) * 12
                const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15)
                const prefix = x.depth > 1 ? '└ ' : ''
                html += `<div class="lineage-link" data-attr-id="${x.id}" style="padding-left: ${indent}px; opacity: ${opacity};">${prefix}${upAttr.fullName}</div>`
            }
        })
        html += `</div>`
    }

    if (sortedDownstream.length > 0) {
        html += `<div class="detail-label">DOWNSTREAM (${sortedDownstream.length})</div>`
        html += `<div class="detail-value">`
        sortedDownstream.forEach((x) => {
            const downAttr = state.attributeLineageMap[x.id]
            if (downAttr) {
                const indent = (x.depth - 1) * 12
                const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15)
                const prefix = x.depth > 1 ? '└ ' : ''
                html += `<div class="lineage-link" data-attr-id="${x.id}" style="padding-left: ${indent}px; opacity: ${opacity};">${prefix}${downAttr.fullName}</div>`
            }
        })
        html += `</div>`
    }

    if (sortedUpstream.length === 0 && sortedDownstream.length === 0) {
        html += `<div class="detail-value text-muted">No lineage connections</div>`
    }

    html += `</div>`

    html += `<div class="lineage-json-view" style="display: none;">
        <div class="detail-label">LINEAGE JSON</div>
        <pre class="lineage-json-pre">${JSON.stringify(lineageJson, null, 2)}</pre>
    </div>`

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
            const attrId = this.getAttribute('data-attr-id')
            if (attrId) selectAttribute(attrId)
        })
    })
}

export function showDatasourceInAttributePanel(ds) {
    state.selectedAttribute = null

    var dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
    var dsLineage = state.datasourceLineageMap[dsId] || { upstream: [], downstream: [] }
    var upstreamList = dsLineage.upstream || []
    var downstreamList = dsLineage.downstream || []

    var connectedIds = new Set()
    upstreamList.forEach(function (x) {
        connectedIds.add(x.id)
    })
    downstreamList.forEach(function (x) {
        connectedIds.add(x.id)
    })

    d3.select('#attribute-graph')
        .selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)
    d3.select('#attribute-graph').selectAll('.edge').classed('edge-highlighted', false).classed('edge-dimmed', false)
    d3.select('#attribute-graph')
        .selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)

    d3.select('#attribute-graph')
        .selectAll('.cluster')
        .each(function () {
            const cluster = d3.select(this)
            const title = cluster.select('title').text()
            if (title && title.startsWith('cluster_')) {
                const clusterId = title.substring(8)
                if (clusterId === dsId) {
                    cluster.classed('cluster-highlighted', true)
                } else if (connectedIds.has(clusterId)) {
                    cluster.classed('cluster-connected', true)
                }
            }
        })

    const col = document.getElementById('attribute-details-col')
    const graphCol = document.getElementById('attribute-graph-col')
    const content = document.getElementById('attribute-details-content')

    graphCol.classList.remove('col-md-12')
    graphCol.classList.add('col-md-8')
    col.style.display = 'block'

    setTimeout(() => {
        if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
    }, 50)

    let html = `<h5>${ds.name}</h5>`

    if (ds.type) {
        html += `<div class="detail-label">TYPE</div>`
        html += `<div class="detail-value"><span class="badge badge-${ds.type}">${ds.type.toUpperCase()}</span></div>`
    }

    if (upstreamList.length > 0) {
        html += '<div class="detail-label">UPSTREAM (' + upstreamList.length + ')</div>'
        html += '<div class="detail-value">'
        upstreamList.forEach(function (item) {
            var depth = item.depth
            var indent = (depth - 1) * 16
            var opacity = Math.max(0.4, 1 - (depth - 1) * 0.2)
            var found = (state.currentConfig.datasources || []).find(function (d) {
                return d.name.replace(/[^a-zA-Z0-9]/g, '_') === item.id
            })
            var name = found ? found.name : item.id
            var prefix = depth > 1 ? '└ ' : ''
            html +=
                '<div class="lineage-link" data-ds-name="' +
                name +
                '" style="padding-left: ' +
                indent +
                'px; opacity: ' +
                opacity +
                ';">' +
                prefix +
                name +
                '</div>'
        })
        html += '</div>'
    }

    if (downstreamList.length > 0) {
        html += '<div class="detail-label">DOWNSTREAM (' + downstreamList.length + ')</div>'
        html += '<div class="detail-value">'
        downstreamList.forEach(function (item) {
            var depth = item.depth
            var indent = (depth - 1) * 16
            var opacity = Math.max(0.4, 1 - (depth - 1) * 0.2)
            var found = (state.currentConfig.datasources || []).find(function (d) {
                return d.name.replace(/[^a-zA-Z0-9]/g, '_') === item.id
            })
            var name = found ? found.name : item.id
            var prefix = depth > 1 ? '└ ' : ''
            html +=
                '<div class="lineage-link" data-ds-name="' +
                name +
                '" style="padding-left: ' +
                indent +
                'px; opacity: ' +
                opacity +
                ';">' +
                prefix +
                name +
                '</div>'
        })
        html += '</div>'
    }

    if (ds.description) {
        html += `<div class="detail-label">Description</div>`
        html += `<div class="detail-value">${ds.description}</div>`
    }

    if (ds.owner) {
        html += `<div class="detail-label">Owner</div>`
        html += `<div class="detail-value">${ds.owner}</div>`
    }

    if (ds.cluster) {
        html += `<div class="detail-label">Cluster</div>`
        html += `<div class="detail-value"><span class="badge badge-cluster">${ds.cluster}</span></div>`
    }

    if (ds.tags && ds.tags.length > 0) {
        html += `<div class="detail-label">Tags</div>`
        html += `<div class="detail-value">${ds.tags
            .map((t) => `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404;">${t}</span>`)
            .join('')}</div>`
    }

    if (ds.attributes && ds.attributes.length > 0) {
        let attrCount = 0
        function countAttrs(attrs) {
            attrs.forEach((attr) => {
                attrCount++
                if (attr.attributes) countAttrs(attr.attributes)
            })
        }
        countAttrs(ds.attributes)

        html += `<div class="detail-label">Attributes (${attrCount})</div>`
        html += `<div class="detail-value">`
        function listAttrs(attrs, indent = 0) {
            attrs.forEach((attr) => {
                const hasChildren = attr.attributes && attr.attributes.length > 0
                html += `<div class="small" style="padding-left: ${indent * 12}px;">${hasChildren ? '▸ ' : ''}${attr.name}</div>`
                if (attr.attributes) listAttrs(attr.attributes, indent + 1)
            })
        }
        listAttrs(ds.attributes)
        html += `</div>`
    }

    if (ds.metadata && Object.keys(ds.metadata).length) {
        html += `<div class="detail-label">Metadata</div>`
        html += `<div class="detail-value">`
        Object.entries(ds.metadata).forEach(([key, value]) => {
            html += `<div class="small"><strong>${key.replace(/_/g, ' ')}:</strong> ${value}</div>`
        })
        html += `</div>`
    }

    if (ds.links && Object.keys(ds.links).length) {
        html += `<div class="links-section">`
        html += `<div class="detail-label">Links</div>`
        Object.entries(ds.links).forEach(([name, url]) => {
            html += `<a href="${url}" target="_blank" class="graph-ctrl-btn link-btn">${name}</a>`
        })
        html += `</div>`
    }

    content.innerHTML = html

    content.querySelectorAll('.lineage-link[data-ds-name]').forEach((el) => {
        el.addEventListener('click', function () {
            const dsName = this.getAttribute('data-ds-name')
            const targetDs = (state.currentConfig.datasources || []).find((d) => d.name === dsName)
            if (targetDs) showDatasourceInAttributePanel(targetDs)
        })
    })
}

export function clearAttributeSelection() {
    state.selectedAttribute = null

    d3.select('#attribute-graph')
        .selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)

    d3.select('#attribute-graph')
        .selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)

    d3.select('#attribute-graph').selectAll('.edge').classed('edge-highlighted', false).classed('edge-dimmed', false)

    const col = document.getElementById('attribute-details-col')
    const graphCol = document.getElementById('attribute-graph-col')
    col.style.display = 'none'
    graphCol.classList.remove('col-md-8')
    graphCol.classList.add('col-md-12')

    setTimeout(() => {
        if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
    }, 50)
}

export function searchAttributes(event) {
    const resultsDiv = document.getElementById('attribute-search-results')
    const items = resultsDiv.querySelectorAll('.search-result-item[data-id]')

    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault()
        if (items.length === 0) return

        const current = resultsDiv.querySelector('.search-result-item.selected')
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
        const selected = resultsDiv.querySelector('.search-result-item.selected')
        if (selected && selected.dataset.id) {
            selectAttributeFromSearch(selected.dataset.id)
        }
        return
    }

    if (event.key === 'Escape') {
        resultsDiv.classList.remove('show')
        resultsDiv.innerHTML = ''
        return
    }

    const query = event.target.value.toLowerCase().trim()

    if (query.length < 2) {
        resultsDiv.classList.remove('show')
        return
    }

    const matches = []
    Object.values(state.attributeLineageMap).forEach((attr) => {
        if (attr.fullName.toLowerCase().includes(query) || attr.name.toLowerCase().includes(query)) {
            matches.push(attr)
        }
    })

    if (matches.length === 0) {
        resultsDiv.classList.remove('show')
        return
    }

    let html = ''
    matches.slice(0, 10).forEach((attr, i) => {
        html += `<div class="search-result-item${i === 0 ? ' selected' : ''}" data-id="${attr.id}" onclick="selectAttributeFromSearch('${attr.id}')">
            <span class="result-type datasource">${attr.datasource}</span>
            <span class="result-name">${attr.name}</span>
        </div>`
    })

    resultsDiv.innerHTML = html
    resultsDiv.classList.add('show')
}

export function selectAttributeFromSearch(attrId) {
    document.getElementById('attribute-search-results').classList.remove('show')
    document.getElementById('attribute-search').value = ''
    selectAttribute(attrId)
}

document.addEventListener('click', function (e) {
    if (!e.target.closest('#attribute-search') && !e.target.closest('#attribute-search-results')) {
        document.getElementById('attribute-search-results')?.classList.remove('show')
    }
})

document.getElementById('attributes-tab')?.addEventListener('shown.bs.tab', function () {
    const newHash = getConfigHash(state.currentConfig)
    if (!state.attributeGraphviz || newHash !== state.attributeLastRenderedConfigHash) {
        setTimeout(() => {
            buildAttributeLineageMap()
            renderAttributeGraph()
            state.attributeLastRenderedConfigHash = newHash
        }, 100)
    }
})
