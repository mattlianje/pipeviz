export const state = {
    currentConfig: null,
    lastRenderedConfigHash: null,
    graphviz: null,
    groupedView: true,
    pipelinesOnlyView: false,
    analysisMode: null, // null, 'critical-path'
    showCostLabels: false,
    expandedGroups: new Set(),
    selectedNode: null,
    focusedNode: null,
    focusedSet: null,
    focusReach: null, // "up:down" hops the focused view was cut to
    // Lineage hops lit around the selection, each way (Infinity = all, 0 = none).
    reachUp: Infinity,
    reachDown: Infinity,
    hideUnrelated: null, // hide (vs dim) off-lineage nodes; null = by graph size
    pendingFocusReset: false,
    viewStateCache: new Map(),
    attributeGraphviz: null,
    attributeLastRenderedConfigHash: null,
    nestedClusterCount: 0,
    lastGraphNodeCount: 0,
    attributeLineageMap: {},
    datasourceLineageMap: {},
    selectedAttribute: null
}

export function getConfigHash(config) {
    return JSON.stringify(config)
}

const currentFocusKey = () => (state.focusedNode ? `${state.focusedNode}:${state.focusReach}` : '')

export function getViewStateKey(expandedGroups = state.expandedGroups, focusKey = currentFocusKey()) {
    const expandedGroupsKey = Array.from(expandedGroups).sort().join(',')
    return `${state.groupedView}|${state.pipelinesOnlyView}|${expandedGroupsKey}|${focusKey}`
}

export function clearViewStateCache() {
    state.viewStateCache.clear()
    state.attributeGraphviz = null
    state.attributeLastRenderedConfigHash = null
}

const MAX_VIEW_CACHE_SIZE = 80

// Evicts dot-only entries first; cached SVGs are expensive to rebuild.
export function addToViewCache(key, value) {
    const cache = state.viewStateCache
    if (!cache.has(key) && cache.size >= MAX_VIEW_CACHE_SIZE) {
        const dotOnly = [...cache].find(([, v]) => !v.svg)
        cache.delete(dotOnly ? dotOnly[0] : cache.keys().next().value)
    }
    cache.set(key, value)
}
