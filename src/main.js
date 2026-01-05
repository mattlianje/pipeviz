// Main entry point for pipeviz
import { initTheme, toggleTheme } from './theme.js'
import { setupTabs, activateTabFromHash } from './tabs.js'
import { setupDragDrop, setupAutoProcess, loadFromUrl, loadExample, loadFromFile, generateShareableUrl } from './loaders.js'
import { filterPipelines, filterDatasources } from './filters.js'
import { renderSplashGraph } from './splash.js'
import { clearSelection, resetGraph, collapseAllGroups, toggleGroup, searchNodes, selectSearchResult } from './graph.js'
import { clearAttributeSelection, resetAttributeGraph, searchAttributes, selectAttributeFromSearch } from './attributes.js'
import { updateDotView, copyDotToClipboard } from './dot.js'

// Expose functions to global scope for onclick handlers in HTML
window.toggleTheme = toggleTheme
window.loadExample = loadExample
window.loadFromFile = loadFromFile
window.generateShareableUrl = generateShareableUrl
window.filterPipelines = filterPipelines
window.filterDatasources = filterDatasources
window.clearSelection = clearSelection
window.resetGraph = resetGraph
window.collapseAllGroups = collapseAllGroups
window.toggleGroup = toggleGroup
window.searchNodes = searchNodes
window.selectSearchResult = selectSearchResult
window.clearAttributeSelection = clearAttributeSelection
window.resetAttributeGraph = resetAttributeGraph
window.searchAttributes = searchAttributes
window.selectAttributeFromSearch = selectAttributeFromSearch
window.updateDotView = updateDotView
window.copyDotToClipboard = copyDotToClipboard

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    initTheme()
    setupTabs()
    activateTabFromHash()
})

// Initialize on window load
window.addEventListener('load', () => {
    setupDragDrop()
    setupAutoProcess()
    loadFromUrl()
    renderSplashGraph()
})

// Handle hash changes
window.addEventListener('hashchange', activateTabFromHash)
