import { state } from './state.js'

const COLORS = ['#6b9dc4', '#a88bc4', '#d4a574', '#7cb47c', '#c98b8b', '#6bb5b5', '#a89080', '#8a9aa8']

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
        container.innerHTML = '<div class="text-muted">Load a configuration to see statistics.</div>'
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
            <div class="stats-section">
                <div class="section-title">Coverage</div>
                <div class="coverage-bars">
                    ${renderCoverageBar('Schedules', stats.coverage.schedules)}
                    ${renderCoverageBar('Airflow Links', stats.coverage.airflow)}
                </div>
            </div>
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

    if (stats.cycles.length > 0) {
        html += `
            <div class="stats-section cycles-warning">
                <div class="section-title">⚠ Cycles Detected <span class="cycle-count">${stats.cycles.length}</span></div>
                <div class="section-hint">Circular dependencies in pipeline graph</div>
                <div class="cycle-list">
                    ${stats.cycles
                        .map(
                            (cycle, i) => `
                        <div class="cycle-item">
                            <span class="cycle-label">#${i + 1}</span>
                            <span class="cycle-path">${cycle.join(' > ')}</span>
                        </div>
                    `
                        )
                        .join('')}
                </div>
            </div>`
    }

    const hasMissing =
        stats.coverage.schedules.missing.length > 0 ||
        stats.coverage.airflow.missing.length > 0 ||
        stats.orphaned.length > 0

    if (hasMissing) {
        html += `
            <div class="stats-section">
                <div class="section-title">Needs Attention</div>
                <div class="missing-scroll">
                    ${renderMissingBlock('Missing Schedule', stats.coverage.schedules.missing, 'pipeline')}
                    ${renderMissingBlock('Missing Airflow Link', stats.coverage.airflow.missing, 'pipeline')}
                    ${renderMissingBlock('Orphaned', stats.orphaned, 'orphan')}
                </div>
            </div>`
    } else if (stats.cycles.length === 0) {
        html += `
            <div class="stats-section all-good">
                <div class="section-title">✓ All Good</div>
                <div class="section-hint">No cycles, coverage gaps, or orphaned nodes.</div>
            </div>`
    }

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

function renderMissingBlock(label, items, type) {
    if (items.length === 0) return ''
    return `<div class="missing-block">
        <div class="missing-header">${label} <span class="missing-count">${items.length}</span></div>
        <div class="missing-names ${type}">${items.join(', ')}</div>
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
