import { state } from "./state.js"
export function generateAttributeDot() {
    if (!state.currentConfig) return ''

    const datasources = state.currentConfig.datasources || []
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark'
    const textColor = '#334155'
    const clusterBg = '#f1f5f9'
    const attrFill = '#ffffff'
    const attrBorder = '#94a3b8'

    // Truncate long names to keep graph clean
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

    // First, collect all source references to know which attrs have downstream connections
    const sourceRefs = new Set()
    function collectSourceRefs(attrs) {
        attrs.forEach(attr => {
            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach(s => sourceRefs.add(s))
            }
            if (attr.attributes) collectSourceRefs(attr.attributes)
        })
    }
    datasources.forEach(ds => {
        if (ds.attributes) collectSourceRefs(ds.attributes)
    })

    // Helper to check if an attribute is referenced as a source
    function isSourceRef(dsName, attrPath) {
        // Check both formats: ds::attr and ds::parent::child
        const ref = `${dsName}::${attrPath.replace(/__/g, '::')}`
        return sourceRefs.has(ref)
    }

    // Track which attributes are structs (have children) for edge routing
    const structAttrs = new Set()

    // Helper to render attributes recursively with nested clusters
    state.nestedClusterCount = 0
    function renderAttributes(attrs, dsName, dsId, prefix = '', depth = 0) {
        let result = ''
        attrs.forEach(attr => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const attrId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const hasUpstream = attr.from ? true : false
            const hasDownstream = isSourceRef(dsName, attrPath)
            const hasLineage = hasUpstream || hasDownstream
            const hasChildren = attr.attributes && attr.attributes.length > 0

            if (hasChildren) {
                // Mark this as a struct for edge routing
                structAttrs.add(attrId)
                // Create a nested cluster for this struct
                state.nestedClusterCount++
                const clusterId = `cluster_${attrId}`
                const nestedBg = hasLineage ? '#dde5ed' : (depth === 0 ? '#e8eef4' : '#dce4ec')
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
                // Invisible anchor node for edge connections (clickable via cluster)
                result += `            "${attrId}" [label="" shape=point width=0 height=0 fixedsize=true style=invis];\n`
                result += renderAttributes(attr.attributes, dsName, dsId, attrPath, depth + 1)
                result += `                        }\n`
            } else {
                const fill = hasLineage ? '#e2e8f0' : attrFill
                result += `            "${attrId}" [label="${attr.name}" shape=box style="filled,rounded" fillcolor="${fill}" color="${attrBorder}" fontcolor="${textColor}" fontsize="9" height="0.3"];\n`
            }
        })
        return result
    }

    // Helper to collect all lineage edges (handles nested paths)
    function collectLineage(attrs, dsId, prefix = '') {
        let edges = []
        attrs.forEach(attr => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const targetId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const targetIsStruct = structAttrs.has(targetId)

            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach(source => {
                    // Parse source::attr or source::parent::child format
                    const parts = source.split('::')
                    if (parts.length >= 2) {
                        const sourceDs = parts[0].replace(/[^a-zA-Z0-9]/g, '_')
                        const sourcePath = parts.slice(1).join('__').replace(/[^a-zA-Z0-9_]/g, '_')
                        const sourceId = `${sourceDs}__${sourcePath}`
                        const sourceIsStruct = structAttrs.has(sourceId)

                        // Build edge with lhead/ltail for struct-to-struct connections
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

            // Recursively collect from nested attributes
            if (attr.attributes && attr.attributes.length > 0) {
                edges = edges.concat(collectLineage(attr.attributes, dsId, attrPath))
            }
        })
        return edges
    }

    // Create subgraphs for each datasource with attributes
    datasources.forEach(ds => {
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

    // Create edges for attribute lineage
    datasources.forEach(ds => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        const edges = collectLineage(ds.attributes, dsId)
        edges.forEach(edge => { dot += edge; })
    })

    dot += `}`
    return dot
}

export function renderAttributeGraph() {
    if (!state.currentConfig) return

    const dot = generateAttributeDot()
    if (!dot) return

    if (!state.attributeGraphviz) {
        // Clear loading indicator
        document.getElementById('attribute-graph').innerHTML = ''
        state.attributeGraphviz = d3.select("#attribute-graph").graphviz()
            .width(document.getElementById('attribute-graph').offsetWidth || 800)
            .height(500)
            .fit(true)
            .zoom(true)
    }

    state.attributeGraphviz.renderDot(dot).on('end', function() {
        // Add click handlers to nodes
        d3.select('#attribute-graph').selectAll('.node').on('click', function(event) {
            event.stopPropagation()
            const title = d3.select(this).select('title').text()
            if (title && state.attributeLineageMap[title]) {
                selectAttribute(title)
            }
        })

        // Add click handlers to clusters (for struct attributes and datasources)
        d3.select('#attribute-graph').selectAll('.cluster').on('click', function(event) {
            event.stopPropagation()
            const title = d3.select(this).select('title').text()
            // Cluster titles are "cluster_id", extract id
            if (title && title.startsWith('cluster_')) {
                const id = title.substring(8)
                // If id contains __, it's an attribute struct; otherwise it's a datasource
                if (id.includes('__') && state.attributeLineageMap[id]) {
                    selectAttribute(id)
                } else {
                    // It's a datasource - find and show its info
                    const dsName = id.replace(/_/g, '-').replace(/--/g, '_'); // rough reverse
                    const ds = (state.currentConfig.datasources || []).find(d =>
                        d.name.replace(/[^a-zA-Z0-9]/g, '_') === id
                    )
                    if (ds) showDatasourceInAttributePanel(ds)
                }
            }
        })

        // Click on graph background to clear selection
        d3.select('#attribute-graph svg').on('click', function(event) {
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

    // Helper to recursively register all attributes (including nested)
    function registerAttributes(attrs, dsName, dsId, prefix = '') {
        attrs.forEach(attr => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const attrId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`
            const fullName = prefix ? `${dsName}::${prefix.replace(/__/g, '::')}::${attr.name}` : `${dsName}::${attr.name}`

            state.attributeLineageMap[attrId] = {
                id: attrId,
                name: attr.name,
                datasource: dsName,
                fullName: fullName,
                upstream: [],
                downstream: []
            }

            // Recursively register nested attributes
            if (attr.attributes && attr.attributes.length > 0) {
                registerAttributes(attr.attributes, dsName, dsId, attrPath)
            }
        })
    }

    // Helper to recursively collect lineage from attributes
    function collectAttributeLineage(attrs, dsId, prefix = '') {
        attrs.forEach(attr => {
            const attrPath = prefix ? `${prefix}__${attr.name}` : attr.name
            const targetId = `${dsId}__${attrPath.replace(/[^a-zA-Z0-9_]/g, '_')}`

            if (attr.from) {
                const sources = Array.isArray(attr.from) ? attr.from : [attr.from]
                sources.forEach(source => {
                    const parts = source.split('::')
                    if (parts.length >= 2) {
                        const sourceDs = parts[0].replace(/[^a-zA-Z0-9]/g, '_')
                        const sourcePath = parts.slice(1).join('__').replace(/[^a-zA-Z0-9_]/g, '_')
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

            // Recursively collect from nested attributes
            if (attr.attributes && attr.attributes.length > 0) {
                collectAttributeLineage(attr.attributes, dsId, attrPath)
            }
        })
    }

    // First pass: create entries for all attributes (including nested)
    datasources.forEach(ds => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        registerAttributes(ds.attributes, ds.name, dsId)
    })

    // Second pass: build lineage relationships
    datasources.forEach(ds => {
        if (!ds.attributes) return
        const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
        collectAttributeLineage(ds.attributes, dsId)
    })

    // Third pass: pre-compute full provenance chains with depth for each attribute
    function computeFullChain(attrId, direction, visited = new Set(), depth = 1) {
        if (visited.has(attrId)) return []
        visited.add(attrId)
        const attr = state.attributeLineageMap[attrId]
        if (!attr) return []
        const next = direction === 'upstream' ? attr.upstream : attr.downstream
        let result = []
        next.forEach(nextId => {
            result.push({ id: nextId, depth: depth })
            result.push(...computeFullChain(nextId, direction, visited, depth + 1))
        })
        return result
    }

    Object.keys(state.attributeLineageMap).forEach(attrId => {
        state.attributeLineageMap[attrId].fullUpstream = computeFullChain(attrId, 'upstream', new Set())
        state.attributeLineageMap[attrId].fullDownstream = computeFullChain(attrId, 'downstream', new Set())
    })
}

export function getFullProvenance(attrId, direction, visited = new Set()) {
    if (visited.has(attrId)) return []
    visited.add(attrId)

    const attr = state.attributeLineageMap[attrId]
    if (!attr) return []

    const next = direction === 'upstream' ? attr.upstream : attr.downstream
    let result = []

    next.forEach(nextId => {
        result.push(nextId)
        result.push(...getFullProvenance(nextId, direction, visited))
    })

    return result
}

export function selectAttribute(attrId) {
    state.selectedAttribute = attrId
    const attr = state.attributeLineageMap[attrId]
    if (!attr) return

    // Clear all previous highlighting first
    d3.select('#attribute-graph').selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)
    d3.select('#attribute-graph').selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)
    d3.select('#attribute-graph').selectAll('.edge')
        .classed('edge-highlighted', false)
        .classed('edge-dimmed', false)

    // Use cached full provenance chains for performance
    const upstream = attr.fullUpstream || []
    const downstream = attr.fullDownstream || []
    const allConnected = new Set([attrId, ...upstream.map(x => x.id), ...downstream.map(x => x.id)])

    // Highlight nodes
    d3.select('#attribute-graph').selectAll('.node').each(function() {
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

    // Highlight clusters (struct attributes)
    d3.select('#attribute-graph').selectAll('.cluster').each(function() {
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

    // Highlight edges
    d3.select('#attribute-graph').selectAll('.edge').each(function() {
        const edge = d3.select(this)
        const title = edge.select('title').text()
        const [from, to] = title.split('->').map(s => s.trim())
        if (allConnected.has(from) && allConnected.has(to)) {
            edge.classed('edge-highlighted', true).classed('edge-dimmed', false)
        } else {
            edge.classed('edge-dimmed', true)
        }
    })

    // Show details panel
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

    // Refit graph after layout change
    setTimeout(() => {
        if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
    }, 50)

    let html = `<h5>${attr.name}</h5>`
    html += `<div class="detail-label">DATASOURCE</div>`
    html += `<div class="detail-value">${attr.datasource}</div>`

    // Find children of this attribute (for structs)
    const childPrefix = attrId + '__'
    const children = Object.keys(state.attributeLineageMap)
        .filter(id => id.startsWith(childPrefix))
        .map(id => {
            const child = state.attributeLineageMap[id]
            const relativePath = id.substring(childPrefix.length)
            const depth = (relativePath.match(/__/g) || []).length
            return { id, name: child.name, relativePath, depth }
        })
        .sort((a, b) => a.relativePath.localeCompare(b.relativePath))

    if (children.length > 0) {
        html += `<div class="detail-label">CONTAINS (${children.length})</div>`
        html += `<div class="detail-value">`
        children.forEach(child => {
            const indent = child.depth * 12
            html += `<div class="lineage-link" data-attr-id="${child.id}" style="padding-left: ${indent}px;">${child.relativePath.replace(/__/g, '.')}</div>`
        })
        html += `</div>`
    }

    // Deduplicate and sort by depth
    function dedupeAndSort(items) {
        const seen = new Map()
        items.forEach(x => {
            if (!seen.has(x.id) || seen.get(x.id).depth > x.depth) {
                seen.set(x.id, x)
            }
        })
        return [...seen.values()].sort((a, b) => a.depth - b.depth)
    }

    const sortedUpstream = dedupeAndSort(upstream)
    const sortedDownstream = dedupeAndSort(downstream)

    if (sortedUpstream.length > 0) {
        html += `<div class="detail-label">UPSTREAM (${sortedUpstream.length})</div>`
        html += `<div class="detail-value">`
        sortedUpstream.forEach(x => {
            const upAttr = state.attributeLineageMap[x.id]
            if (upAttr) {
                const indent = (x.depth - 1) * 12
                const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15)
                html += `<div class="lineage-link" data-attr-id="${x.id}" style="padding-left: ${indent}px; opacity: ${opacity};">${upAttr.fullName}</div>`
            }
        })
        html += `</div>`
    }

    if (sortedDownstream.length > 0) {
        html += `<div class="detail-label">DOWNSTREAM (${sortedDownstream.length})</div>`
        html += `<div class="detail-value">`
        sortedDownstream.forEach(x => {
            const downAttr = state.attributeLineageMap[x.id]
            if (downAttr) {
                const indent = (x.depth - 1) * 12
                const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15)
                html += `<div class="lineage-link" data-attr-id="${x.id}" style="padding-left: ${indent}px; opacity: ${opacity};">${downAttr.fullName}</div>`
            }
        })
        html += `</div>`
    }

    if (sortedUpstream.length === 0 && sortedDownstream.length === 0) {
        html += `<div class="detail-value text-muted">No lineage connections</div>`
    }

    content.innerHTML = html

    // Add click handlers via delegation
    content.querySelectorAll('.lineage-link').forEach(el => {
        el.addEventListener('click', function() {
            const attrId = this.getAttribute('data-attr-id')
            if (attrId) selectAttribute(attrId)
        })
    })
}

export function showDatasourceInAttributePanel(ds) {
    state.selectedAttribute = null

    // Clear highlighting
    d3.select('#attribute-graph').selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)
    d3.select('#attribute-graph').selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)

    // Highlight this datasource cluster
    const dsId = ds.name.replace(/[^a-zA-Z0-9]/g, '_')
    d3.select('#attribute-graph').selectAll('.cluster').each(function() {
        const cluster = d3.select(this)
        const title = cluster.select('title').text()
        if (title === `cluster_${dsId}`) {
            cluster.classed('cluster-highlighted', true)
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
    html += `<div class="detail-label">Type</div>`
    html += `<div class="detail-value"><span class="badge bg-secondary">Data Source</span></div>`

    if (ds.type) {
        html += `<div class="detail-label">Source Type</div>`
        html += `<div class="detail-value"><span class="badge badge-${ds.type}">${ds.type.toUpperCase()}</span></div>`
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
        html += `<div class="detail-value">${ds.tags.map(t =>
            `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404;">${t}</span>`
        ).join('')}</div>`
    }

    if (ds.attributes && ds.attributes.length > 0) {
        let attrCount = 0
        function countAttrs(attrs) {
            attrs.forEach(attr => {
                attrCount++
                if (attr.attributes) countAttrs(attr.attributes)
            })
        }
        countAttrs(ds.attributes)

        html += `<div class="detail-label">Attributes (${attrCount})</div>`
        html += `<div class="detail-value">`
        function listAttrs(attrs, indent = 0) {
            attrs.forEach(attr => {
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
            html += `<a href="${url}" target="_blank" class="btn btn-sm btn-outline-primary link-btn">${name}</a>`
        })
        html += `</div>`
    }

    content.innerHTML = html
}

export function clearAttributeSelection() {
    state.selectedAttribute = null

    d3.select('#attribute-graph').selectAll('.node')
        .classed('node-highlighted', false)
        .classed('node-connected', false)
        .classed('node-dimmed', false)

    d3.select('#attribute-graph').selectAll('.cluster')
        .classed('cluster-highlighted', false)
        .classed('cluster-connected', false)
        .classed('cluster-dimmed', false)

    d3.select('#attribute-graph').selectAll('.edge')
        .classed('edge-highlighted', false)
        .classed('edge-dimmed', false)

    const col = document.getElementById('attribute-details-col')
    const graphCol = document.getElementById('attribute-graph-col')
    col.style.display = 'none'
    graphCol.classList.remove('col-md-8')
    graphCol.classList.add('col-md-12')

    // Refit graph after layout change
    setTimeout(() => {
        if (state.attributeGraphviz) state.attributeGraphviz.fit(true)
    }, 50)
}

export function searchAttributes(event) {
    const resultsDiv = document.getElementById('attribute-search-results')
    const items = resultsDiv.querySelectorAll('.search-result-item[data-id]')

    // Handle arrow key navigation
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

        items.forEach(item => item.classList.remove('selected'))
        items[index].classList.add('selected')
        items[index].scrollIntoView({ block: 'nearest' })
        return
    }

    // Handle Enter key
    if (event.key === 'Enter') {
        const selected = resultsDiv.querySelector('.search-result-item.selected')
        if (selected && selected.dataset.id) {
            selectAttributeFromSearch(selected.dataset.id)
        }
        return
    }

    // Handle Escape key
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
    Object.values(state.attributeLineageMap).forEach(attr => {
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

// Close search dropdown when clicking outside
document.addEventListener('click', function(e) {
    if (!e.target.closest('#attribute-search') && !e.target.closest('#attribute-search-results')) {
        document.getElementById('attribute-search-results')?.classList.remove('show')
    }
})

// Set up attributes tab listener
document.getElementById('attributes-tab')?.addEventListener('shown.bs.tab', function() {
    setTimeout(() => {
        buildAttributeLineageMap()
        renderAttributeGraph()
    }, 100)
})

