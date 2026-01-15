export function setupTabs() {
    document.querySelectorAll('#dashboardTabs button').forEach((tab) => {
        tab.addEventListener('shown.bs.tab', function (e) {
            const tabs = document.getElementById('dashboardTabs')
            const tabName = e.target.id.replace('-tab', '')
            if (e.target.id === 'home-tab') {
                tabs.classList.add('hidden-on-home')
                history.replaceState(null, '', window.location.pathname + window.location.search)
            } else {
                tabs.classList.remove('hidden-on-home')
                // Preserve node parameter if present
                const nodeParam = getNodeFromHash()
                const newHash = nodeParam ? `${tabName}&node=${encodeURIComponent(nodeParam)}` : tabName
                history.replaceState(null, '', '#' + newHash)
            }
        })
    })
}

export function parseHash() {
    const hash = window.location.hash.slice(1)
    if (!hash) return { tab: null, node: null, view: null, pipelines: null }

    const parts = hash.split('&')
    let tab = null
    let node = null
    let view = null
    let pipelines = null

    parts.forEach((part) => {
        if (part.startsWith('node=')) {
            node = decodeURIComponent(part.slice(5))
        } else if (part.startsWith('view=')) {
            view = decodeURIComponent(part.slice(5))
        } else if (part.startsWith('pipelines=')) {
            pipelines = decodeURIComponent(part.slice(10))
                .split(',')
                .filter((p) => p)
        } else if (!part.includes('=')) {
            tab = part
        }
    })

    return { tab, node, view, pipelines }
}

export function getNodeFromHash() {
    return parseHash().node
}

export function getPlannerStateFromHash() {
    const { view, pipelines } = parseHash()
    return { view, pipelines }
}

export function updateHashWithNode(nodeName) {
    const { tab } = parseHash()
    const currentTab = tab || 'graph'
    if (nodeName) {
        history.replaceState(null, '', `#${currentTab}&node=${encodeURIComponent(nodeName)}`)
    } else {
        history.replaceState(null, '', `#${currentTab}`)
    }
}

export function updateHashWithPlannerState(view, pipelines) {
    const currentTab = 'backfill'
    let hash = `#${currentTab}`
    if (view) {
        hash += `&view=${encodeURIComponent(view)}`
    }
    if (pipelines && pipelines.length > 0) {
        hash += `&pipelines=${pipelines.map((p) => encodeURIComponent(p)).join(',')}`
    }
    history.replaceState(null, '', hash)
}

export function activateTabFromHash() {
    const { tab, node } = parseHash()
    if (tab) {
        const tabEl = document.getElementById(tab + '-tab')
        if (tabEl) {
            tabEl.click()
        }
    }
    // Node selection is handled by graph.js after graph renders
}
