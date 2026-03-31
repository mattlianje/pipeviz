import { initTheme, toggleTheme } from './theme.js'
import {
    setupDragDrop,
    setupAutoProcess,
    loadFromUrl,
    loadExample,
    loadFromFile,
    applyConfig,
    formatJson
} from './loaders.js'
import { filterPipelines, filterDatasources } from './filters.js'
import {
    clearSelection,
    resetGraph,
    toggleCollapseAll,
    toggleGroup,
    togglePipelinesOnly,
    searchNodes,
    selectSearchResult,
    showBlastRadius,
    selectNodeFromHash,
    restoreBlastRadiusFromHash,
    hideBlastRadiusModal,
    generateGraphvizDot,
    focusNode,
    unfocusNode
} from './graph.js'
import {
    clearAttributeSelection,
    resetAttributeGraph,
    renderAttributeGraph,
    buildAttributeLineageMap,
    searchAttributes,
    selectAttributeFromSearch
} from './attributes.js'
import {
    populatePlannerSelect,
    updatePlannerPlan,
    copyPlannerToClipboard,
    setPlannerView,
    clearPlannerSelection,
    filterPlannerItems,
    generateMermaidExport
} from './export.js'
import { renderStats } from './stats.js'

window.toggleTheme = toggleTheme
window.loadExample = loadExample
window.loadFromFile = loadFromFile
window.applyConfig = applyConfig
window.formatJson = formatJson
window.filterPipelines = filterPipelines
window.filterDatasources = filterDatasources
window.clearSelection = clearSelection
window.resetGraph = resetGraph
window.toggleCollapseAll = toggleCollapseAll
window.toggleGroup = toggleGroup
window.togglePipelinesOnly = togglePipelinesOnly
window.searchNodes = searchNodes
window.selectSearchResult = selectSearchResult
window.showBlastRadius = showBlastRadius
window.hideBlastRadiusModal = hideBlastRadiusModal
window.selectNodeFromHash = selectNodeFromHash
window.restoreBlastRadiusFromHash = restoreBlastRadiusFromHash
window.focusNode = focusNode
window.unfocusNode = unfocusNode

window.clearAttributeSelection = clearAttributeSelection
window.resetAttributeGraph = resetAttributeGraph
window.searchAttributes = searchAttributes
window.selectAttributeFromSearch = selectAttributeFromSearch

window.populatePlannerSelect = populatePlannerSelect
window.updatePlannerPlan = updatePlannerPlan
window.copyPlannerToClipboard = copyPlannerToClipboard
window.copyPlannerOutput = copyPlannerToClipboard
window.setPlannerView = setPlannerView
window.clearPlannerSelection = clearPlannerSelection
window.filterPlannerItems = filterPlannerItems

window.toggleSpecModal = function () {
    const modal = document.getElementById('spec-modal')
    modal.classList.toggle('show')
}

window.toggleJsonPanel = function () {
    document.getElementById('json-panel').classList.toggle('hidden')
}

document.addEventListener(
    'pointerdown',
    function (e) {
        const panel = document.getElementById('json-panel')
        if (!panel || panel.classList.contains('hidden')) return
        if (panel.contains(e.target)) return
        if (e.target.closest('.json-panel-handle')) return
        panel.classList.add('hidden')
    },
    true
)

window.switchPanelTab = function (tab) {
    // Update tab buttons
    document.querySelectorAll('.json-panel-tab').forEach((t) => t.classList.remove('active'))
    document.querySelector(`.json-panel-tab[onclick*="${tab}"]`)?.classList.add('active')

    // Show/hide content areas
    const editor = document.getElementById('json-input')
    const mermaidPane = document.getElementById('panel-export-mermaid')
    const dotPane = document.getElementById('panel-export-dot')
    const footer = document.querySelector('.json-panel-footer')

    editor.style.display = tab === 'json' ? '' : 'none'
    mermaidPane.classList.toggle('active', tab === 'mermaid')
    dotPane.classList.toggle('active', tab === 'dot')
    footer.style.display = tab === 'json' ? '' : 'none'

    // Populate export content
    if (tab === 'mermaid') {
        const output = document.getElementById('panel-mermaid-output')
        output.textContent = generateMermaidExport() || 'Load a configuration first...'
    } else if (tab === 'dot') {
        const output = document.getElementById('panel-dot-output')
        output.textContent = generateGraphvizDot() || 'Load a configuration first...'
    }
}

window.copyPanelExport = function (format) {
    const id = format === 'mermaid' ? 'panel-mermaid-output' : 'panel-dot-output'
    const text = document.getElementById(id)?.textContent
    if (!text) return

    const btn = document.querySelector(`#panel-export-${format} .json-panel-copy`)
    navigator.clipboard.writeText(text).then(() => {
        if (btn) {
            const orig = btn.textContent
            btn.textContent = 'Copied!'
            setTimeout(() => {
                btn.textContent = orig
            }, 1500)
        }
    })
}

document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'j') {
        e.preventDefault()
        window.toggleJsonPanel()
    }
    if (e.key === 'Escape') {
        const jsonPanel = document.getElementById('json-panel')
        if (jsonPanel && !jsonPanel.classList.contains('hidden')) {
            jsonPanel.classList.add('hidden')
            return
        }
        const specModal = document.getElementById('spec-modal')
        if (specModal?.classList.contains('show')) {
            specModal.classList.remove('show')
        }
        const blastModal = document.getElementById('blast-radius-modal')
        if (blastModal?.classList.contains('show')) {
            blastModal.classList.remove('show')
        }
    }
})

window.switchView = function (paneId) {
    document.querySelectorAll('.view-pane').forEach((p) => p.classList.remove('active'))
    document.querySelectorAll('.tab-strip-btn').forEach((b) => b.classList.remove('active'))

    const pane = document.getElementById(paneId)
    if (pane) pane.classList.add('active')

    const btn = document.querySelector(`.tab-strip-btn[onclick*="${paneId}"]`)
    if (btn) btn.classList.add('active')

    if (paneId === 'stats-pane') renderStats()
}

window.switchGraphSubview = function (subview) {
    document.querySelectorAll('.graph-subview').forEach((s) => s.classList.remove('active'))
    document.querySelectorAll('.graph-subtab').forEach((b) => b.classList.remove('active'))

    const pipelineToolbar = document.getElementById('pipeline-toolbar-items')
    const attributeToolbar = document.getElementById('attribute-toolbar-items')

    if (subview === 'attributes') {
        document.getElementById('attributes-subview').classList.add('active')
        document.querySelectorAll('.graph-subtab')[1].classList.add('active')
        pipelineToolbar.style.display = 'none'
        attributeToolbar.style.display = 'contents'
        buildAttributeLineageMap()
        renderAttributeGraph()
    } else {
        document.getElementById('pipelines-subview').classList.add('active')
        document.querySelectorAll('.graph-subtab')[0].classList.add('active')
        pipelineToolbar.style.display = 'contents'
        attributeToolbar.style.display = 'none'
    }
}

window.promptGitHubToken = function () {
    const existing = localStorage.getItem('pipeviz-gh-token')
    const token = prompt(
        'Paste a GitHub personal access token (with repo read access).\n\n' +
            'Create one at: GitHub → Settings → Developer settings → Personal access tokens',
        existing || ''
    )
    if (token !== null) {
        if (token.trim()) {
            localStorage.setItem('pipeviz-gh-token', token.trim())
        } else {
            localStorage.removeItem('pipeviz-gh-token')
        }
        window.location.reload()
    }
}

window.clearGitHubToken = function () {
    localStorage.removeItem('pipeviz-gh-token')
    const statusDiv = document.getElementById('json-status')
    if (statusDiv) statusDiv.innerHTML = '<span class="success">GitHub token cleared</span>'
}

document.addEventListener('DOMContentLoaded', () => {
    initTheme()
})

window.addEventListener('load', () => {
    setupDragDrop()
    setupAutoProcess()
    loadFromUrl()
})

window.addEventListener('hashchange', () => {
    selectNodeFromHash()
    restoreBlastRadiusFromHash()
})
