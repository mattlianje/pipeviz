import { state } from './state.js'

let cachedCriticalPath = null
let cachedCriticalPathHash = null
let cachedCostliestPath = null
let cachedCostliestPathHash = null

function getCachedCriticalPath() {
    if (!state.currentConfig) return null
    const hash = JSON.stringify(state.currentConfig.pipelines?.map((p) => [p.name, p.duration]))
    if (cachedCriticalPathHash !== hash) {
        cachedCriticalPathHash = hash
        cachedCriticalPath = computeCriticalPath(state.currentConfig.pipelines || [])
    }
    return cachedCriticalPath
}

function getCachedCostliestPath() {
    if (!state.currentConfig) return null
    const hash = JSON.stringify(state.currentConfig.pipelines?.map((p) => [p.name, p.cost]))
    if (cachedCostliestPathHash !== hash) {
        cachedCostliestPathHash = hash
        cachedCostliestPath = computeCostliestPath(state.currentConfig.pipelines || [])
    }
    return cachedCostliestPath
}

export function getCriticalPathNodes() {
    const cp = getCachedCriticalPath()
    if (!cp || !cp.path) return new Set()
    return new Set(cp.path.map((p) => p.name))
}

export function getCriticalPathEdges() {
    const cp = getCachedCriticalPath()
    if (!cp || !cp.path || cp.path.length < 2) return new Set()

    const edges = new Set()
    for (let i = 0; i < cp.path.length - 1; i++) {
        edges.add(`${cp.path[i].name}|${cp.path[i + 1].name}`)
    }
    return edges
}

export function getCostliestPathNodes() {
    const cp = getCachedCostliestPath()
    if (!cp || !cp.path) return new Set()
    return new Set(cp.path.map((p) => p.name))
}

export function getCostliestPathEdges() {
    const cp = getCachedCostliestPath()
    if (!cp || !cp.path || cp.path.length < 2) return new Set()

    const edges = new Set()
    for (let i = 0; i < cp.path.length - 1; i++) {
        edges.add(`${cp.path[i].name}|${cp.path[i + 1].name}`)
    }
    return edges
}

const COLORS = ['#6b9dc4', '#a88bc4', '#d4a574', '#7cb47c', '#c98b8b', '#6bb5b5', '#a89080', '#8a9aa8']

function computeCriticalPath(pipelines) {
    const pipelineMap = new Map()
    pipelines.forEach((p) => pipelineMap.set(p.name, p))

    const hasDurations = pipelines.some((p) => p.duration !== undefined && p.duration !== null)
    if (!hasDurations) return null

    const graph = new Map() // node -> [downstream nodes]
    const inDegree = new Map()
    const pipelineNames = new Set(pipelines.map((p) => p.name))

    pipelines.forEach((p) => {
        if (!graph.has(p.name)) graph.set(p.name, [])
        if (!inDegree.has(p.name)) inDegree.set(p.name, 0)
    })

    const outputToProducer = new Map()
    pipelines.forEach((p) => {
        p.output_sources?.forEach((ds) => outputToProducer.set(ds, p.name))
    })

    pipelines.forEach((p) => {
        p.upstream_pipelines?.forEach((upstream) => {
            if (pipelineNames.has(upstream)) {
                graph.get(upstream).push(p.name)
                inDegree.set(p.name, inDegree.get(p.name) + 1)
            }
        })
        p.input_sources?.forEach((ds) => {
            const producer = outputToProducer.get(ds)
            if (producer && producer !== p.name && !p.upstream_pipelines?.includes(producer)) {
                graph.get(producer).push(p.name)
                inDegree.set(p.name, inDegree.get(p.name) + 1)
            }
        })
    })

    const est = new Map()
    const eft = new Map()
    const predecessor = new Map()

    pipelines.forEach((p) => {
        if (inDegree.get(p.name) === 0) {
            est.set(p.name, 0)
            eft.set(p.name, p.duration || 0)
        }
    })

    const queue = pipelines.filter((p) => inDegree.get(p.name) === 0).map((p) => p.name)
    const processed = []

    while (queue.length > 0) {
        const current = queue.shift()
        processed.push(current)

        graph.get(current).forEach((neighbor) => {
            const newEst = eft.get(current)
            if (!est.has(neighbor) || newEst > est.get(neighbor)) {
                est.set(neighbor, newEst)
                predecessor.set(neighbor, current)
            }
            const neighborPipeline = pipelineMap.get(neighbor)
            const neighborDuration = neighborPipeline?.duration || 0
            eft.set(neighbor, est.get(neighbor) + neighborDuration)

            inDegree.set(neighbor, inDegree.get(neighbor) - 1)
            if (inDegree.get(neighbor) === 0) {
                queue.push(neighbor)
            }
        })
    }

    let maxEft = 0
    let criticalEnd = null
    eft.forEach((time, node) => {
        if (time > maxEft) {
            maxEft = time
            criticalEnd = node
        }
    })

    if (!criticalEnd) return null

    const criticalPath = []
    let current = criticalEnd
    let totalCost = 0
    while (current) {
        const pipeline = pipelineMap.get(current)
        const nodeCost = pipeline?.cost || 0
        totalCost += nodeCost
        criticalPath.unshift({
            name: current,
            duration: pipeline?.duration || 0,
            cost: nodeCost,
            est: est.get(current) || 0,
            eft: eft.get(current) || 0
        })
        current = predecessor.get(current)
    }

    const slack = new Map()
    const criticalNodes = new Set(criticalPath.map((n) => n.name))

    pipelines.forEach((p) => {
        if (criticalNodes.has(p.name)) {
            slack.set(p.name, 0)
        } else if (est.has(p.name)) {
            slack.set(p.name, maxEft - eft.get(p.name))
        }
    })

    return {
        totalDuration: maxEft,
        totalCost,
        path: criticalPath,
        pipelinesWithDuration: pipelines.filter((p) => p.duration !== undefined).length,
        pipelinesWithCost: pipelines.filter((p) => p.cost !== undefined).length,
        totalPipelines: pipelines.length,
        slack
    }
}

function computeCostliestPath(pipelines) {
    const pipelineMap = new Map()
    pipelines.forEach((p) => pipelineMap.set(p.name, p))

    const hasCosts = pipelines.some((p) => p.cost !== undefined && p.cost !== null)
    if (!hasCosts) return null

    const graph = new Map()
    const inDegree = new Map()
    const pipelineNames = new Set(pipelines.map((p) => p.name))

    pipelines.forEach((p) => {
        if (!graph.has(p.name)) graph.set(p.name, [])
        if (!inDegree.has(p.name)) inDegree.set(p.name, 0)
    })

    const outputToProducer = new Map()
    pipelines.forEach((p) => {
        p.output_sources?.forEach((ds) => outputToProducer.set(ds, p.name))
    })

    pipelines.forEach((p) => {
        p.upstream_pipelines?.forEach((upstream) => {
            if (pipelineNames.has(upstream)) {
                graph.get(upstream).push(p.name)
                inDegree.set(p.name, inDegree.get(p.name) + 1)
            }
        })
        p.input_sources?.forEach((ds) => {
            const producer = outputToProducer.get(ds)
            if (producer && producer !== p.name && !p.upstream_pipelines?.includes(producer)) {
                graph.get(producer).push(p.name)
                inDegree.set(p.name, inDegree.get(p.name) + 1)
            }
        })
    })

    const costToNode = new Map()
    const predecessor = new Map()

    pipelines.forEach((p) => {
        if (inDegree.get(p.name) === 0) {
            costToNode.set(p.name, p.cost || 0)
        }
    })

    const queue = pipelines.filter((p) => inDegree.get(p.name) === 0).map((p) => p.name)
    const inDegreeCopy = new Map(inDegree)

    while (queue.length > 0) {
        const current = queue.shift()
        const currentCost = costToNode.get(current) || 0

        graph.get(current).forEach((neighbor) => {
            const neighborPipeline = pipelineMap.get(neighbor)
            const neighborCost = neighborPipeline?.cost || 0
            const newCost = currentCost + neighborCost

            if (!costToNode.has(neighbor) || newCost > costToNode.get(neighbor)) {
                costToNode.set(neighbor, newCost)
                predecessor.set(neighbor, current)
            }

            inDegreeCopy.set(neighbor, inDegreeCopy.get(neighbor) - 1)
            if (inDegreeCopy.get(neighbor) === 0) {
                queue.push(neighbor)
            }
        })
    }

    let maxCost = 0
    let costliestEnd = null
    costToNode.forEach((cost, node) => {
        if (cost > maxCost) {
            maxCost = cost
            costliestEnd = node
        }
    })

    if (!costliestEnd) return null

    const costliestPath = []
    let current = costliestEnd
    let totalDuration = 0
    while (current) {
        const pipeline = pipelineMap.get(current)
        totalDuration += pipeline?.duration || 0
        costliestPath.unshift({
            name: current,
            cost: pipeline?.cost || 0,
            duration: pipeline?.duration || 0
        })
        current = predecessor.get(current)
    }

    return {
        totalCost: maxCost,
        totalDuration,
        path: costliestPath,
        pipelinesWithCost: pipelines.filter((p) => p.cost !== undefined).length,
        totalPipelines: pipelines.length
    }
}

function detectCycles(pipelines) {
    const graph = new Map()
    const pipelineNames = new Set()

    pipelines.forEach((p) => {
        const nodeName = p.group || p.name
        pipelineNames.add(nodeName)
        if (!graph.has(nodeName)) graph.set(nodeName, new Set())
    })

    pipelines.forEach((p) => {
        const nodeName = p.group || p.name
        p.upstream_pipelines?.forEach((upstream) => {
            if (pipelineNames.has(upstream)) {
                if (!graph.has(upstream)) graph.set(upstream, new Set())
                graph.get(upstream).add(nodeName)
            }
        })
    })

    const cycles = []
    const visited = new Set()
    const recStack = new Set()

    function dfs(node, path) {
        visited.add(node)
        recStack.add(node)
        path.push(node)

        for (const neighbor of graph.get(node) || new Set()) {
            if (!visited.has(neighbor)) {
                const result = dfs(neighbor, path)
                if (result) return result
            } else if (recStack.has(neighbor)) {
                const cycleStart = path.indexOf(neighbor)
                const cycle = path.slice(cycleStart)
                cycle.push(neighbor)
                return cycle
            }
        }

        path.pop()
        recStack.delete(node)
        return null
    }

    for (const node of pipelineNames) {
        if (!visited.has(node)) {
            const cycle = dfs(node, [])
            if (cycle) {
                cycles.push(cycle)
                visited.clear()
                recStack.clear()
                cycle.forEach((n) => visited.add(n))
            }
        }
    }

    return cycles
}

export function computeStats() {
    if (!state.currentConfig) return null

    const pipelines = state.currentConfig.pipelines || []
    const datasources = state.currentConfig.datasources || []
    const cycles = detectCycles(pipelines)
    const criticalPath = computeCriticalPath(pipelines)
    const costliestPath = computeCostliestPath(pipelines)

    const upstreamCounts = new Map()
    const downstreamCounts = new Map()
    const groups = new Set()

    pipelines.forEach((p) => {
        if (p.group) groups.add(p.group)
    })

    pipelines.forEach((p) => {
        const nodeName = p.group || p.name
        p.input_sources?.forEach((s) => {
            downstreamCounts.set(s, (downstreamCounts.get(s) || 0) + 1)
            upstreamCounts.set(nodeName, (upstreamCounts.get(nodeName) || 0) + 1)
        })
        p.output_sources?.forEach((s) => {
            downstreamCounts.set(nodeName, (downstreamCounts.get(nodeName) || 0) + 1)
            upstreamCounts.set(s, (upstreamCounts.get(s) || 0) + 1)
        })
        p.upstream_pipelines?.forEach((u) => {
            downstreamCounts.set(u, (downstreamCounts.get(u) || 0) + 1)
            upstreamCounts.set(nodeName, (upstreamCounts.get(nodeName) || 0) + 1)
        })
    })

    const pipelineNames = new Set(pipelines.map((p) => p.name))
    const allNodes = new Set([...upstreamCounts.keys(), ...downstreamCounts.keys()])
    const hubs = [...allNodes]
        .map((name) => {
            let type = 'datasource'
            if (groups.has(name)) type = 'group'
            else if (pipelineNames.has(name)) type = 'pipeline'
            return {
                name,
                type,
                upstream: upstreamCounts.get(name) || 0,
                downstream: downstreamCounts.get(name) || 0,
                total: (upstreamCounts.get(name) || 0) + (downstreamCounts.get(name) || 0)
            }
        })
        .sort((a, b) => b.total - a.total)
        .slice(0, 8)

    const referencedSources = new Set()
    pipelines.forEach((p) => {
        p.input_sources?.forEach((s) => referencedSources.add(s))
        p.output_sources?.forEach((s) => referencedSources.add(s))
    })
    const orphaned = datasources.filter((ds) => !referencedSources.has(ds.name)).map((ds) => ds.name)

    const clusterCounts = {}
    pipelines.forEach((p) => {
        const cluster = p.cluster || 'unclustered'
        clusterCounts[cluster] = (clusterCounts[cluster] || 0) + 1
    })

    const typeCounts = {}
    datasources.forEach((ds) => {
        const type = ds.type || 'unknown'
        typeCounts[type] = (typeCounts[type] || 0) + 1
    })

    return {
        counts: {
            pipelines: pipelines.length,
            datasources: datasources.length,
            clusters: Object.keys(clusterCounts).filter((c) => c !== 'unclustered').length
        },
        cycles,
        criticalPath,
        costliestPath,
        hubs,
        orphaned,
        coverage: {
            schedules: {
                covered: pipelines.filter((p) => p.schedule).length,
                total: pipelines.length,
                missing: pipelines.filter((p) => !p.schedule).map((p) => p.name)
            },
            airflow: {
                covered: pipelines.filter((p) => p.links?.airflow).length,
                total: pipelines.length,
                missing: pipelines.filter((p) => !p.links?.airflow).map((p) => p.name)
            }
        },
        distributions: { clusters: clusterCounts, types: typeCounts }
    }
}

export function renderStats() {
    const container = document.getElementById('stats-content')
    if (!container) return

    const stats = computeStats()
    if (!stats) {
        container.innerHTML = '<div style="color: var(--text-muted);">Load a configuration to see statistics.</div>'
        return
    }

    const clusterData = Object.entries(stats.distributions.clusters)
        .sort((a, b) => b[1] - a[1])
        .map(([label, value]) => ({ label, value }))
    const typeData = Object.entries(stats.distributions.types)
        .sort((a, b) => b[1] - a[1])
        .map(([label, value]) => ({ label, value }))

    let html = `
        <div class="stats-summary">
            <span><strong>${stats.counts.pipelines}</strong> pipelines</span>
            <span><strong>${stats.counts.datasources}</strong> datasources</span>
            <span><strong>${stats.counts.clusters}</strong> clusters</span>
        </div>
        <div class="stats-row">
            <div class="chart-box">
                <div class="chart-title">Pipelines by Cluster</div>
                <div class="pie-container">
                    ${renderPieChart(clusterData, 120)}
                    <div class="pie-legend">${renderPieLegend(clusterData)}</div>
                </div>
            </div>
            <div class="chart-box">
                <div class="chart-title">Datasources by Type</div>
                <div class="pie-container">
                    ${renderPieChart(typeData, 120)}
                    <div class="pie-legend">${renderPieLegend(typeData)}</div>
                </div>
            </div>
        </div>
        <div class="stats-row">
            ${
                stats.hubs.length > 0
                    ? `
                <div class="stats-section">
                    <div class="section-title">Hubs <span class="section-hint">most connected</span></div>
                    <div class="hub-list">
                        ${stats.hubs
                            .map(
                                (h, i) => `
                            <div class="hub-row">
                                <span class="hub-rank">${i + 1}</span>
                                <span class="hub-name ${h.type}">${h.name}</span>
                                <span class="hub-stats">↑${h.upstream} ↓${h.downstream}</span>
                            </div>
                        `
                            )
                            .join('')}
                    </div>
                </div>
            `
                    : ''
            }
        </div>`

    container.innerHTML = html
}

function renderCoverageBar(label, data) {
    const pct = data.total > 0 ? Math.round((data.covered / data.total) * 100) : 100
    const color = pct >= 80 ? 'good' : pct >= 50 ? 'okay' : 'low'
    return `<div class="coverage-row">
        <span class="coverage-label">${label}</span>
        <div class="coverage-bar-wrap"><div class="coverage-bar ${color}" style="width: ${pct}%"></div></div>
        <span class="coverage-pct ${color}">${pct}%</span>
    </div>`
}

function renderPieChart(data, size = 80) {
    if (!data?.length) return ''
    const total = data.reduce((sum, d) => sum + d.value, 0)
    if (total === 0) return ''

    let currentAngle = -90
    let paths = ''

    data.forEach((d, i) => {
        const angle = (d.value / total) * 360
        const startRad = (currentAngle * Math.PI) / 180
        const endRad = ((currentAngle + angle) * Math.PI) / 180
        const r = size / 2 - 1,
            cx = size / 2,
            cy = size / 2
        const x1 = cx + r * Math.cos(startRad),
            y1 = cy + r * Math.sin(startRad)
        const x2 = cx + r * Math.cos(endRad),
            y2 = cy + r * Math.sin(endRad)
        const color = COLORS[i % COLORS.length]

        paths +=
            data.length === 1
                ? `<circle cx="${cx}" cy="${cy}" r="${r}" fill="${color}" />`
                : `<path d="M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${angle > 180 ? 1 : 0} 1 ${x2} ${y2} Z" fill="${color}" />`
        currentAngle += angle
    })

    return `<svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">${paths}</svg>`
}

function renderPieLegend(data) {
    const total = data.reduce((sum, d) => sum + d.value, 0)
    return data
        .map((d, i) => {
            const pct = total > 0 ? Math.round((d.value / total) * 100) : 0
            return `<div class="pie-legend-row">
            <span class="pie-legend-dot" style="background:${COLORS[i % COLORS.length]}"></span>
            <span class="pie-legend-name">${d.label}</span>
            <span class="pie-legend-val">${d.value} (${pct}%)</span>
        </div>`
        })
        .join('')
}
