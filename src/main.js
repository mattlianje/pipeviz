import { initTheme, toggleTheme } from './theme.js'
import { setupTabs, activateTabFromHash } from './tabs.js'
import {
    setupDragDrop,
    setupAutoProcess,
    loadFromUrl,
    loadExample,
    loadFromFile,
    generateShareableUrl
} from './loaders.js'
import { filterPipelines, filterDatasources } from './filters.js'
import { renderSplashGraph } from './splash.js'
import {
    clearSelection,
    resetGraph,
    collapseAllGroups,
    toggleCollapseAll,
    toggleGroup,
    togglePipelinesOnly,
    setAnalysisMode,
    toggleAnalysisMode,
    toggleCostLabels,
    searchNodes,
    selectSearchResult,
    showBlastRadius,
    selectNodeFromHash,
    toggleOverflowMenu,
    updateOverflowMenuState,
    toggleAnalysisModeFromMenu,
    toggleCostLabelsFromMenu
} from './graph.js'
import {
    clearAttributeSelection,
    resetAttributeGraph,
    searchAttributes,
    selectAttributeFromSearch
} from './attributes.js'
import { updateDotView, copyDotToClipboard } from './dot.js'
import {
    updateExportView,
    copyExportToClipboard,
    setExportFormat,
    populateBackfillSelect,
    updateBackfillPlan,
    copyBackfillToClipboard,
    setBackfillView,
    toggleBackfillPicker,
    clearBackfillSelection,
    filterBackfillPipelines,
    restorePlannerStateFromHash
} from './export.js'
import { renderStats } from './stats.js'

window.toggleTheme = toggleTheme
window.loadExample = loadExample
window.loadFromFile = loadFromFile
window.generateShareableUrl = generateShareableUrl
window.filterPipelines = filterPipelines
window.filterDatasources = filterDatasources
window.clearSelection = clearSelection
window.resetGraph = resetGraph
window.collapseAllGroups = collapseAllGroups
window.toggleCollapseAll = toggleCollapseAll
window.toggleGroup = toggleGroup
window.togglePipelinesOnly = togglePipelinesOnly
window.setAnalysisMode = setAnalysisMode
window.toggleAnalysisMode = toggleAnalysisMode
window.toggleCostLabels = toggleCostLabels
window.searchNodes = searchNodes
window.selectSearchResult = selectSearchResult
window.showBlastRadius = showBlastRadius
window.clearAttributeSelection = clearAttributeSelection
window.resetAttributeGraph = resetAttributeGraph
window.searchAttributes = searchAttributes
window.selectAttributeFromSearch = selectAttributeFromSearch
window.updateDotView = updateDotView
window.copyDotToClipboard = copyDotToClipboard
window.updateExportView = updateExportView
window.copyExportToClipboard = copyExportToClipboard
window.setExportFormat = setExportFormat
window.populateBackfillSelect = populateBackfillSelect
window.updateBackfillPlan = updateBackfillPlan
window.copyBackfillToClipboard = copyBackfillToClipboard
window.setBackfillView = setBackfillView
window.toggleBackfillPicker = toggleBackfillPicker
window.clearBackfillSelection = clearBackfillSelection
window.filterBackfillPipelines = filterBackfillPipelines
window.selectNodeFromHash = selectNodeFromHash
window.restorePlannerStateFromHash = restorePlannerStateFromHash
window.toggleOverflowMenu = toggleOverflowMenu
window.updateOverflowMenuState = updateOverflowMenuState
window.toggleAnalysisModeFromMenu = toggleAnalysisModeFromMenu
window.toggleCostLabelsFromMenu = toggleCostLabelsFromMenu

document.addEventListener('DOMContentLoaded', () => {
    initTheme()
    setupTabs()
    activateTabFromHash()
    document.getElementById('stats-tab')?.addEventListener('shown.bs.tab', renderStats)
    document.getElementById('backfill-tab')?.addEventListener('shown.bs.tab', restorePlannerStateFromHash)
})

window.addEventListener('load', () => {
    setupDragDrop()
    setupAutoProcess()
    loadFromUrl()
    renderSplashGraph()
})

window.addEventListener('hashchange', () => {
    activateTabFromHash()
    // Also try to select node from hash (handles back/forward navigation)
    selectNodeFromHash()
    // Also try to restore planner state from hash
    restorePlannerStateFromHash()
})
