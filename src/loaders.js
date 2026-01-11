import { state, exampleConfig } from './state.js'
import { renderPipelines, renderDatasources } from './tables.js'
import { updateFilters } from './filters.js'
import { renderGraph } from './graph.js'
import { updateDotView } from './dot.js'
import { updateExportView, populateBackfillSelect } from './export.js'
import { renderStats } from './stats.js'

export function loadExample() {
    document.getElementById('json-input').value = JSON.stringify(exampleConfig, null, 2)
    loadJson()
}

export function generateShareableUrl() {
    if (!state.currentConfig) {
        alert('No configuration loaded to share')
        return
    }

    const configJson = JSON.stringify(state.currentConfig)
    const encodedConfig = btoa(configJson)
    const currentUrl = window.location.origin + window.location.pathname
    const shareableUrl = `${currentUrl}?config=${encodedConfig}`

    navigator.clipboard
        .writeText(shareableUrl)
        .then(() => {
            const statusDiv = document.getElementById('json-status')
            statusDiv.innerHTML = '<div class="alert alert-success">Shareable URL copied to clipboard!</div>'
            setTimeout(() => {
                statusDiv.innerHTML = ''
            }, 3000)
        })
        .catch(() => {
            prompt('Copy this shareable URL:', shareableUrl)
        })
}

async function tryLoadPipevizJson() {
    try {
        const response = await fetch('pipeviz.json')
        if (response.ok) {
            const configText = await response.text()
            const parsed = JSON.parse(configText)
            document.getElementById('json-input').value = JSON.stringify(parsed, null, 2)
            loadJson()
            const statusDiv = document.getElementById('json-status')
            statusDiv.innerHTML = '<div class="alert alert-success">Loaded pipeviz.json</div>'
            setTimeout(() => {
                statusDiv.innerHTML = ''
            }, 3000)
            return true
        }
    } catch (error) {}
    return false
}

export async function loadFromUrl() {
    const urlParams = new URLSearchParams(window.location.search)

    const configParam = urlParams.get('config')
    const viewParam = urlParams.get('view')
    if (configParam) {
        try {
            const decodedConfig = atob(configParam)
            const parsed = JSON.parse(decodedConfig)
            document.getElementById('json-input').value = JSON.stringify(parsed, null, 2)
            loadJson()
            const targetView = viewParam || 'graph'
            document.getElementById(targetView + '-tab')?.click()
            return
        } catch (error) {
            console.error('Error decoding config parameter:', error)
        }
    }

    const urlParam = urlParams.get('url')
    const autoView = urlParams.get('view') || (urlParam ? 'graph' : null)

    if (urlParam) {
        fetch(urlParam)
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`)
                }
                return response.text()
            })
            .then((configText) => {
                try {
                    const parsed = JSON.parse(configText)
                    document.getElementById('json-input').value = JSON.stringify(parsed, null, 2)
                    loadJson()
                    if (autoView) {
                        document.getElementById(autoView + '-tab')?.click()
                    }
                } catch (error) {
                    document.getElementById('json-input').value = configText
                    loadJson()
                }
            })
            .catch((error) => {
                const statusDiv = document.getElementById('json-status')
                statusDiv.innerHTML = `<div class="alert alert-danger">Error loading from URL: ${error.message}</div>`
                if (!state.currentConfig) {
                    loadExample()
                }
            })
        return
    }

    const loaded = await tryLoadPipevizJson()
    if (!loaded && !state.currentConfig) {
        loadExample()
    }
}

export function setupDragDrop() {
    const dropZone = document.getElementById('drop-zone')
    const textarea = document.getElementById('json-input')

    ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach((eventName) => {
        dropZone.addEventListener(eventName, preventDefaults, false)
    })

    function preventDefaults(e) {
        e.preventDefault()
        e.stopPropagation()
    }

    ;['dragenter', 'dragover'].forEach((eventName) => {
        dropZone.addEventListener(eventName, () => dropZone.classList.add('dragover'), false)
    })
    ;['dragleave', 'drop'].forEach((eventName) => {
        dropZone.addEventListener(eventName, () => dropZone.classList.remove('dragover'), false)
    })

    dropZone.addEventListener('drop', handleDrop, false)

    function handleDrop(e) {
        const files = e.dataTransfer.files
        if (files.length > 0) {
            const file = files[0]
            if (file.type === 'application/json' || file.name.endsWith('.json')) {
                const reader = new FileReader()
                reader.onload = function (e) {
                    try {
                        const parsed = JSON.parse(e.target.result)
                        textarea.value = JSON.stringify(parsed, null, 2)
                        loadJson()
                    } catch (error) {
                        textarea.value = e.target.result
                        loadJson()
                    }
                }
                reader.readAsText(file)
            } else {
                const statusDiv = document.getElementById('json-status')
                statusDiv.innerHTML = '<div class="alert alert-warning">Please drop a JSON file</div>'
            }
        }
    }
}

export function loadFromFile(event) {
    const file = event.target.files[0]
    if (file && (file.type === 'application/json' || file.name.endsWith('.json'))) {
        const reader = new FileReader()
        reader.onload = function (e) {
            try {
                const parsed = JSON.parse(e.target.result)
                document.getElementById('json-input').value = JSON.stringify(parsed, null, 2)
                loadJson()
            } catch (error) {
                document.getElementById('json-input').value = e.target.result
                loadJson()
            }
        }
        reader.readAsText(file)
    } else {
        const statusDiv = document.getElementById('json-status')
        statusDiv.innerHTML = '<div class="alert alert-warning">Please select a valid JSON file</div>'
    }
}

export function setupAutoProcess() {
    const textarea = document.getElementById('json-input')
    const statusDiv = document.getElementById('json-status')
    let timeout

    textarea.addEventListener('input', function () {
        clearTimeout(timeout)
        timeout = setTimeout(() => {
            const content = textarea.value.trim()
            if (!content) {
                statusDiv.innerHTML = ''
                return
            }
            try {
                JSON.parse(content)
                statusDiv.innerHTML =
                    '<div class="alert alert-success py-1 px-2" style="font-size: 0.85em;">Valid JSON</div>'
                loadJson()
            } catch (e) {
                statusDiv.innerHTML = `<div class="alert alert-danger py-1 px-2" style="font-size: 0.85em;">Invalid JSON: ${e.message}</div>`
            }
        }, 500)
    })
}

export function loadJson() {
    const jsonText = document.getElementById('json-input').value.trim()
    const statusDiv = document.getElementById('json-status')

    if (!jsonText) {
        statusDiv.innerHTML = '<div class="alert alert-warning">Please enter JSON configuration</div>'
        return
    }

    try {
        state.currentConfig = JSON.parse(jsonText)
        statusDiv.innerHTML = '<div class="alert alert-success">Configuration loaded successfully!</div>'

        renderPipelines()
        renderDatasources()
        updateFilters()
        renderGraph()
        updateDotView()
        updateExportView()
        populateBackfillSelect()
        renderStats()
    } catch (error) {
        statusDiv.innerHTML = `<div class="alert alert-danger">JSON Parse Error: ${error.message}</div>`
    }
}
