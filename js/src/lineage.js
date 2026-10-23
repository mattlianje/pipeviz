import { state } from './state.js'

// Lineage is derived from the config rather than read off the rendered SVG, so
// it doesn't depend on which edges are drawn or what focus has filtered out.
// Traversal runs over member pipelines and projects onto what's on screen
// (collapsed groups), so passing through a group only follows the members that
// actually connect.

let modelConfig = null
const models = new Map()

// Pipeline -> pipeline dependencies. `data`: the upstream writes a datasource the
// downstream reads. `explicit`: an upstream_pipelines entry with no shared
// datasource behind it (e.g. "run after").
export function pipelineDependencies(pipelines) {
    const names = new Set(pipelines.map((p) => p.name))
    const groupMembers = new Map()
    pipelines.forEach((p) => {
        if (!p.group) return
        if (!groupMembers.has(p.group)) groupMembers.set(p.group, [])
        groupMembers.get(p.group).push(p.name)
    })
    const producers = new Map()
    pipelines.forEach((p) =>
        p.output_sources?.forEach((s) => {
            if (!producers.has(s)) producers.set(s, [])
            producers.get(s).push(p.name)
        })
    )

    const deps = new Map()
    pipelines.forEach((p) =>
        p.input_sources?.forEach((s) =>
            (producers.get(s) || []).forEach((q) => {
                if (q !== p.name) deps.set(`${q}\0${p.name}`, { from: q, to: p.name, kind: 'data' })
            })
        )
    )
    pipelines.forEach((p) =>
        p.upstream_pipelines?.forEach((ref) => {
            // A reference may name a whole group.
            const ups = names.has(ref) ? [ref] : groupMembers.get(ref) || []
            ups.forEach((u) => {
                const key = `${u}\0${p.name}`
                if (u !== p.name && !deps.has(key)) deps.set(key, { from: u, to: p.name, kind: 'explicit' })
            })
        })
    )
    return [...deps.values()]
}

const modelKey = () => `${state.groupedView}|${state.pipelinesOnlyView}|${[...state.expandedGroups].sort().join(',')}`

function buildModel() {
    const pipelines = state.currentConfig?.pipelines || []
    const pipelineNames = new Set(pipelines.map((p) => p.name))

    // Member -> on-screen node name.
    const vis = new Map()
    const members = new Map()
    pipelines.forEach((p) => {
        const collapsed = state.groupedView && p.group && !state.expandedGroups.has(p.group)
        const v = collapsed ? p.group : p.name
        vis.set(p.name, v)
        if (collapsed) {
            if (!members.has(v)) members.set(v, [])
            members.get(v).push(p.name)
        }
    })
    const visOf = (n) => vis.get(n) ?? n

    const down = new Map()
    const up = new Map()
    // On-screen edges, keyed "from\0to"; a data edge wins over an explicit one.
    const edges = new Map()
    const link = (from, to, kind) => {
        if (!down.has(from)) down.set(from, [])
        if (!up.has(to)) up.set(to, [])
        down.get(from).push(to)
        up.get(to).push(from)
        const a = visOf(from)
        const b = visOf(to)
        if (a === b) return
        const key = `${a}\0${b}`
        if (!edges.has(key) || edges.get(key).kind === 'explicit') edges.set(key, { from: a, to: b, kind })
    }

    const deps = pipelineDependencies(pipelines)
    if (state.pipelinesOnlyView) {
        deps.forEach((d) => link(d.from, d.to, d.kind))
    } else {
        pipelines.forEach((p) => {
            p.input_sources?.forEach((s) => link(s, p.name, 'data'))
            p.output_sources?.forEach((s) => link(p.name, s, 'data'))
        })
        deps.filter((d) => d.kind === 'explicit').forEach((d) => link(d.from, d.to, 'explicit'))
    }

    // Everything this view can draw, by on-screen name.
    const nodes = new Set(pipelines.map((p) => visOf(p.name)))
    if (!state.pipelinesOnlyView) {
        ;(state.currentConfig?.datasources || []).forEach((ds) => nodes.add(ds.name))
        pipelines.forEach((p) => [...(p.input_sources || []), ...(p.output_sources || [])].forEach((s) => nodes.add(s)))
    }

    const isPipeline = (n) => pipelineNames.has(n) || members.has(n)
    return { down, up, visOf, members, nodes, edges: [...edges.values()], isPipeline }
}

export function getLineageModel() {
    if (modelConfig !== state.currentConfig) {
        models.clear()
        modelConfig = state.currentConfig
    }
    const key = modelKey()
    if (!models.has(key)) models.set(key, buildModel())
    return models.get(key)
}

// On-screen node -> hop distance, walking `adj` from every member of `start`.
// Steps that stay inside one on-screen node (e.g. within a collapsed group)
// cost nothing, so hops match what you count on screen.
function hopDistances(model, adj, start) {
    const { visOf } = model
    const dist = new Map()
    let layer = model.members.get(start) || [start]
    layer.forEach((n) => dist.set(n, 0))
    let d = 0
    while (layer.length) {
        const further = []
        for (let i = 0; i < layer.length; i++) {
            const n = layer[i]
            for (const m of adj.get(n) || []) {
                if (dist.has(m)) continue
                if (visOf(m) === visOf(n)) {
                    dist.set(m, d)
                    layer.push(m)
                } else {
                    further.push(m)
                }
            }
        }
        d++
        layer = []
        for (const m of further) {
            if (dist.has(m)) continue
            dist.set(m, d)
            layer.push(m)
        }
    }

    // A member of a collapsed group doesn't list its own group.
    const self = visOf(start)
    const out = new Map()
    for (const [n, h] of dist) {
        const v = visOf(n)
        if (v === start || v === self) continue
        if (!out.has(v) || h < out.get(v)) out.set(v, h)
    }
    return out
}

// { up, down }: on-screen node -> hops from `name`, plus the deepest hop each way.
export function lineageHops(name) {
    const model = getLineageModel()
    const up = hopDistances(model, model.up, name)
    const down = hopDistances(model, model.down, name)
    let maxUp = 0
    let maxDown = 0
    for (const h of up.values()) if (h > maxUp) maxUp = h
    for (const h of down.values()) if (h > maxDown) maxDown = h
    return { up, down, maxUp, maxDown }
}

// Lineage of `name` cut to the current reach (state.reachUp / reachDown).
export function litLineage(name) {
    const hops = lineageHops(name)
    const within = (m, reach) => new Map([...m].filter(([, h]) => h <= reach))
    return { ...hops, up: within(hops.up, state.reachUp), down: within(hops.down, state.reachDown), all: hops }
}
