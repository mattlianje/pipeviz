// Core pipeviz graph logic — pure functions, no I/O.

export function getPipelines(config) {
    return config?.pipelines || []
}

export function getDatasources(config) {
    return config?.datasources || []
}

export function allNodeNames(config) {
    const names = new Set()
    getPipelines(config).forEach((p) => names.add(p.name))
    getDatasources(config).forEach((d) => names.add(d.name))
    getPipelines(config).forEach((p) => {
        p.input_sources?.forEach((s) => names.add(s))
        p.output_sources?.forEach((s) => names.add(s))
    })
    return names
}

export function buildMaps(config) {
    const upstream = {}
    const downstream = {}

    getPipelines(config).forEach((p) => {
        p.input_sources?.forEach((src) => {
            ;(downstream[src] = downstream[src] || []).push(p.name)
            ;(upstream[p.name] = upstream[p.name] || []).push(src)
        })
        p.output_sources?.forEach((out) => {
            ;(downstream[p.name] = downstream[p.name] || []).push(out)
            ;(upstream[out] = upstream[out] || []).push(p.name)
        })
        p.upstream_pipelines?.forEach((up) => {
            ;(downstream[up] = downstream[up] || []).push(p.name)
            ;(upstream[p.name] = upstream[p.name] || []).push(up)
        })
    })
    return { upstream, downstream }
}

export function walk(start, map, maxDepth = 50) {
    const visited = new Set()
    const result = []
    function dfs(node, depth) {
        if (visited.has(node) || depth > maxDepth) return
        visited.add(node)
        const neighbors = map[node] || []
        neighbors.forEach((n) => {
            result.push({ name: n, depth })
            dfs(n, depth + 1)
        })
    }
    dfs(start, 1)
    return result
}

export function topoSort(nodes, downstreamMap) {
    const nodeSet = new Set(nodes)
    const inDegree = {}
    const adj = {}
    nodeSet.forEach((n) => {
        inDegree[n] = 0
        adj[n] = []
    })
    nodeSet.forEach((n) => {
        ;(downstreamMap[n] || []).forEach((d) => {
            if (nodeSet.has(d)) {
                adj[n].push(d)
                inDegree[d] = (inDegree[d] || 0) + 1
            }
        })
    })
    const queue = Object.keys(inDegree).filter((n) => inDegree[n] === 0)
    const sorted = []
    while (queue.length) {
        const n = queue.shift()
        sorted.push(n)
        adj[n]?.forEach((d) => {
            inDegree[d]--
            if (inDegree[d] === 0) queue.push(d)
        })
    }
    return sorted
}

// --- Tool implementations ---

export function nodeInfo(config, name) {
    if (!config) return { error: 'No config loaded. Set PIPEVIZ_CONFIG or pass a JSON path.' }

    const pipeline = getPipelines(config).find((p) => p.name === name)
    if (pipeline) {
        return { ...pipeline, node_type: 'pipeline', _group_members: undefined }
    }

    const ds = getDatasources(config).find((d) => d.name === name)
    if (ds) return { ...ds, node_type: 'datasource' }

    const all = allNodeNames(config)
    if (all.has(name)) return { node_type: 'datasource', name, auto_created: true }

    return { error: `Node "${name}" not found.` }
}

export function upstreamOf(config, name, depth) {
    if (!config) return { error: 'No config loaded.' }
    const { upstream } = buildMaps(config)
    const chain = walk(name, upstream, depth || 50)
    const pipelineSet = new Set(getPipelines(config).map((p) => p.name))
    const pipelines = chain.filter((n) => pipelineSet.has(n.name))
    const sources = chain.filter((n) => !pipelineSet.has(n.name))
    return { node: name, upstream_pipelines: pipelines, upstream_sources: sources, total: chain.length }
}

export function downstreamOf(config, name, depth) {
    if (!config) return { error: 'No config loaded.' }
    const { downstream } = buildMaps(config)
    const chain = walk(name, downstream, depth || 50)
    const pipelineSet = new Set(getPipelines(config).map((p) => p.name))
    const pipelines = chain.filter((n) => pipelineSet.has(n.name))
    const sources = chain.filter((n) => !pipelineSet.has(n.name))
    return { node: name, downstream_pipelines: pipelines, downstream_sources: sources, total: chain.length }
}

export function blastRadius(config, name) {
    if (!config) return { error: 'No config loaded.' }
    const { downstream } = buildMaps(config)
    const chain = walk(name, downstream)
    const pipelineNames = new Set(getPipelines(config).map((p) => p.name))
    const seenPipelines = []
    const seenSources = []
    const seen = new Set()
    chain.forEach((n) => {
        if (seen.has(n.name)) return
        seen.add(n.name)
        if (pipelineNames.has(n.name)) seenPipelines.push(n.name)
        else seenSources.push(n.name)
    })

    const affectedOwners = new Set()
    const allNodes = [...getPipelines(config), ...getDatasources(config)]
    seen.forEach((n) => {
        const node = allNodes.find((x) => x.name === n)
        if (node) getOwners(node).forEach((o) => affectedOwners.add(o))
    })

    return {
        node: name,
        affected_pipelines: seenPipelines,
        affected_sources: seenSources,
        affected_owners: [...affectedOwners],
        total_affected: seenPipelines.length + seenSources.length,
        summary: `${seenPipelines.length} pipeline(s) and ${seenSources.length} source(s) downstream of "${name}".${affectedOwners.size > 0 ? ` Owners to notify: ${[...affectedOwners].join(', ')}.` : ''}`
    }
}

function collectEdges(startNodes, downstreamMap) {
    const seen = new Set()
    const edges = []
    function collect(node, visited = new Set()) {
        if (visited.has(node)) return
        visited.add(node)
        const neighbors = downstreamMap[node] || []
        neighbors.forEach((n) => {
            if (!seen.has(`${node}|${n}`)) {
                seen.add(`${node}|${n}`)
                edges.push([node, n])
            }
            collect(n, visited)
        })
    }
    startNodes.forEach((n) => collect(n))
    return edges
}

function renderMermaid(affectedSet, edges, originNodes, pipelineNames) {
    const filteredEdges = edges.filter(([a, b]) => affectedSet.has(a) && affectedSet.has(b))
    const nodeIds = new Map()
    let idCounter = 0
    function nodeId(n) {
        if (!nodeIds.has(n)) nodeIds.set(n, `n${idCounter++}`)
        return nodeIds.get(n)
    }

    const lines = ['graph LR']
    affectedSet.forEach((n) => {
        const id = nodeId(n)
        if (originNodes.has(n)) lines.push(`    ${id}[["${n}"]]:::origin`)
        else if (pipelineNames.has(n)) lines.push(`    ${id}("${n}"):::pipeline`)
        else lines.push(`    ${id}[("${n}")]:::source`)
    })
    filteredEdges.forEach(([a, b]) => {
        lines.push(`    ${nodeId(a)} --> ${nodeId(b)}`)
    })
    lines.push('    classDef origin fill:#ff6b35,stroke:#e55a2b,color:#fff')
    lines.push('    classDef pipeline fill:#1a73e8,stroke:#1557b0,color:#fff')
    lines.push('    classDef source fill:#34a853,stroke:#2d8f47,color:#fff')
    return lines.join('\n')
}

export function blastRadiusDiagram(config, name) {
    if (!config) return { error: 'No config loaded.' }
    const { downstream } = buildMaps(config)
    const chain = walk(name, downstream)
    if (chain.length === 0) return { node: name, mermaid: `graph LR\n    n0[["${name} (no downstream)"]]:::origin\n    classDef origin fill:#ff6b35,stroke:#e55a2b,color:#fff`, total_affected: 0 }

    const pipelineNames = new Set(getPipelines(config).map((p) => p.name))
    const affectedSet = new Set([name, ...chain.map((n) => n.name)])
    const edges = collectEdges([name], downstream)

    return {
        node: name,
        mermaid: renderMermaid(affectedSet, edges, new Set([name]), pipelineNames),
        total_affected: affectedSet.size - 1
    }
}

export function fileBlastRadiusDiagram(config, filePath) {
    if (!config) return { error: 'No config loaded.' }

    const normalised = filePath.replace(/^.*?\/blob\/[^/]+\//, '').replace(/^\/+/, '')
    const matched = getPipelines(config).filter((p) => {
        if (!p.links) return false
        return Object.values(p.links).some((url) => {
            const urlPath = url.replace(/^.*?\/blob\/[^/]+\//, '')
            return urlPath === normalised || normalised.endsWith(urlPath) || urlPath.endsWith(normalised)
        })
    })

    if (matched.length === 0) {
        const basename = filePath.split('/').pop()
        return { file: filePath, matched_pipelines: [], mermaid: `graph LR\n    n0["${basename} (no matches)"]`, total_affected: 0 }
    }

    const { downstream } = buildMaps(config)
    const pipelineNames = new Set(getPipelines(config).map((p) => p.name))
    const originNames = new Set(matched.map((p) => p.name))
    const affectedSet = new Set(originNames)

    matched.forEach((p) => {
        walk(p.name, downstream).forEach((n) => affectedSet.add(n.name))
    })

    const edges = collectEdges([...originNames], downstream)

    return {
        file: filePath,
        matched_pipelines: [...originNames],
        mermaid: renderMermaid(affectedSet, edges, originNames, pipelineNames),
        total_affected: affectedSet.size
    }
}

export function backfillOrder(config, name) {
    if (!config) return { error: 'No config loaded.' }
    const { downstream } = buildMaps(config)
    const chain = walk(name, downstream)
    const pipelineNames = new Set(getPipelines(config).map((p) => p.name))
    const affectedPipelines = [name, ...chain.map((n) => n.name)].filter((n) => pipelineNames.has(n))
    const sorted = topoSort(affectedPipelines, downstream)

    return {
        starting_from: name,
        backfill_order: sorted.map((n, i) => ({
            stage: i + 1,
            pipeline: n,
            schedule: getPipelines(config).find((p) => p.name === n)?.schedule || null
        })),
        total_stages: sorted.length
    }
}

export function clusterInfo(config, clusterName) {
    if (!config) return { error: 'No config loaded.' }
    const pipelines = getPipelines(config).filter((p) => p.cluster === clusterName)
    const sources = getDatasources(config).filter((d) => d.cluster === clusterName)
    if (pipelines.length === 0 && sources.length === 0) {
        return { error: `Cluster "${clusterName}" not found or empty.` }
    }
    return {
        cluster: clusterName,
        pipelines: pipelines.map((p) => p.name),
        datasources: sources.map((d) => d.name),
        description: config.clusters?.find((c) => c.name === clusterName)?.description || null
    }
}

export function fileBlastRadius(config, filePath) {
    if (!config) return { error: 'No config loaded.' }

    const normalised = filePath.replace(/^.*?\/blob\/[^/]+\//, '').replace(/^\/+/, '')

    const matched = getPipelines(config).filter((p) => {
        if (!p.links) return false
        return Object.values(p.links).some((url) => {
            const urlPath = url.replace(/^.*?\/blob\/[^/]+\//, '')
            return urlPath === normalised || normalised.endsWith(urlPath) || urlPath.endsWith(normalised)
        })
    })

    if (matched.length === 0) {
        return { file: filePath, matched_pipelines: [], total_affected: 0, summary: `No pipelines linked to "${filePath}".` }
    }

    const { downstream } = buildMaps(config)
    const pipelineNames = new Set(getPipelines(config).map((p) => p.name))
    const allAffectedPipelines = new Set()
    const allAffectedSources = new Set()

    matched.forEach((p) => {
        allAffectedPipelines.add(p.name)
        const chain = walk(p.name, downstream)
        chain.forEach((n) => {
            if (pipelineNames.has(n.name)) allAffectedPipelines.add(n.name)
            else allAffectedSources.add(n.name)
        })
    })

    const directNames = matched.map((p) => p.name)
    const downstreamPipelines = [...allAffectedPipelines].filter((n) => !directNames.includes(n))

    return {
        file: filePath,
        matched_pipelines: directNames,
        downstream_pipelines: downstreamPipelines,
        affected_sources: [...allAffectedSources],
        total_affected: allAffectedPipelines.size + allAffectedSources.size,
        summary: `"${filePath}" maps to ${directNames.length} pipeline(s). ${allAffectedPipelines.size} pipeline(s) and ${allAffectedSources.size} source(s) in blast radius.`
    }
}

function getOwners(node) {
    if (node.owners) return Array.isArray(node.owners) ? node.owners : [node.owners]
    if (node.owner) return [node.owner]
    return []
}

function getUsers(node) {
    if (node.users) return Array.isArray(node.users) ? node.users : [node.users]
    return []
}

export function ownersOf(config, name) {
    if (!config) return { error: 'No config loaded.' }
    const pipeline = getPipelines(config).find((p) => p.name === name)
    if (pipeline) return { node: name, node_type: 'pipeline', owners: getOwners(pipeline), users: getUsers(pipeline) }
    const ds = getDatasources(config).find((d) => d.name === name)
    if (ds) return { node: name, node_type: 'datasource', owners: getOwners(ds), users: getUsers(ds) }
    const all = allNodeNames(config)
    if (all.has(name)) return { node: name, node_type: 'datasource', auto_created: true, owners: [], users: [] }
    return { error: `Node "${name}" not found.` }
}

export function nodesByPerson(config, person) {
    if (!config) return { error: 'No config loaded.' }
    const q = person.toLowerCase()
    const owns = []
    const uses = []
    getPipelines(config).forEach((p) => {
        if (getOwners(p).some((o) => o.toLowerCase().includes(q))) owns.push({ name: p.name, type: 'pipeline' })
        if (getUsers(p).some((u) => u.toLowerCase().includes(q))) uses.push({ name: p.name, type: 'pipeline' })
    })
    getDatasources(config).forEach((d) => {
        if (getOwners(d).some((o) => o.toLowerCase().includes(q))) owns.push({ name: d.name, type: 'datasource' })
        if (getUsers(d).some((u) => u.toLowerCase().includes(q))) uses.push({ name: d.name, type: 'datasource' })
    })
    return { person, owns, uses, total_owns: owns.length, total_uses: uses.length }
}

export function searchNodes(config, query) {
    if (!config) return { error: 'No config loaded.' }
    const q = query.toLowerCase()
    const results = []

    getPipelines(config).forEach((p) => {
        if (p.name.toLowerCase().includes(q)) results.push({ name: p.name, type: 'pipeline' })
    })
    getDatasources(config).forEach((d) => {
        if (d.name.toLowerCase().includes(q)) results.push({ name: d.name, type: 'datasource' })
    })
    allNodeNames(config).forEach((name) => {
        if (name.toLowerCase().includes(q) && !results.some((r) => r.name === name)) {
            results.push({ name, type: 'datasource' })
        }
    })

    return { query, results, count: results.length }
}

export function graphSummary(config) {
    if (!config) return { error: 'No config loaded.' }
    const pipelines = getPipelines(config)
    const sources = getDatasources(config)
    const groups = new Set()
    const clusters = new Set()
    pipelines.forEach((p) => {
        if (p.group) groups.add(p.group)
        if (p.cluster) clusters.add(p.cluster)
    })

    return {
        pipelines: pipelines.length,
        datasources: sources.length,
        groups: [...groups],
        clusters: [...clusters],
        pipeline_names: pipelines.map((p) => p.name),
        datasource_names: sources.map((d) => d.name)
    }
}
