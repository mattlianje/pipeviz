import { state, exampleConfig, clearViewStateCache } from './state.js'
import { renderPipelines, renderDatasources } from './tables.js'
import { updateFilters } from './filters.js'
import { renderGraph } from './graph.js'
import { populatePlannerSelect } from './export.js'
import { renderStats } from './stats.js'

export function loadExample() {
    document.getElementById('json-input').value = JSON.stringify(exampleConfig, null, 2)
    loadJson()
}

export function applyConfig() {
    loadJson()
}

export function formatJson() {
    const textarea = document.getElementById('json-input')
    try {
        const parsed = JSON.parse(textarea.value)
        textarea.value = JSON.stringify(parsed, null, 2)
    } catch (e) {
        const statusDiv = document.getElementById('json-status')
        statusDiv.innerHTML = '<span class="error">Invalid JSON: ' + e.message + '</span>'
    }
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
            statusDiv.innerHTML = '<span class="success">Shareable URL copied to clipboard!</span>'
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
            statusDiv.innerHTML = '<span class="success">Loaded pipeviz.json</span>'
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
            return
        } catch (error) {
            console.error('Error decoding config parameter:', error)
        }
    }

    const ghParam = urlParams.get('gh')
    let urlParam = urlParams.get('url')

    if (ghParam && !urlParam) {
        const parts = ghParam.split('/')
        if (parts.length === 2) {
            urlParam = `https://raw.githubusercontent.com/${parts[0]}/${parts[1]}/main/pipeviz.json`
        } else if (parts.length === 3) {
            urlParam = `https://raw.githubusercontent.com/${parts[0]}/${parts[1]}/${parts[2]}/pipeviz.json`
        } else if (parts.length > 3) {
            urlParam = `https://raw.githubusercontent.com/${ghParam}`
        }
    }

    if (urlParam) {
        const tokenParam = urlParams.get('token') || localStorage.getItem('pipeviz-gh-token')
        let fetchUrl = urlParam
        const fetchHeaders = {}

        const isGitHubUrl = urlParam.includes('raw.githubusercontent.com') || urlParam.includes('github.com')
        if (tokenParam && isGitHubUrl) {
            fetchHeaders['Authorization'] = `token ${tokenParam}`
            const ghRawMatch = urlParam.match(/^https:\/\/raw\.githubusercontent\.com\/([^/]+)\/([^/]+)\/(.+)$/)
            if (ghRawMatch) {
                const [, owner, repo, rest] = ghRawMatch
                const slashIdx = rest.indexOf('/')
                const ref = rest.substring(0, slashIdx)
                const path = rest.substring(slashIdx + 1)
                fetchUrl = `https://api.github.com/repos/${owner}/${repo}/contents/${path}?ref=${ref}`
                fetchHeaders['Accept'] = 'application/vnd.github.v3.raw'
            }
            if (urlParams.get('token')) {
                localStorage.setItem('pipeviz-gh-token', urlParams.get('token'))
            }
        }

        fetch(fetchUrl, Object.keys(fetchHeaders).length ? { headers: fetchHeaders } : {})
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
                } catch (error) {
                    document.getElementById('json-input').value = configText
                    loadJson()
                }
            })
            .catch((error) => {
                const banner = document.getElementById('auth-banner')
                if (isGitHubUrl && !tokenParam) {
                    banner.innerHTML = `<div class="auth-banner">This looks like a private GitHub repo. <a onclick="promptGitHubToken(); return false">Add a GitHub token</a> to load it.<button class="auth-dismiss" onclick="this.parentElement.parentElement.innerHTML=''">&times;</button></div>`
                } else if (isGitHubUrl && (error.message.includes('401') || error.message.includes('403'))) {
                    localStorage.removeItem('pipeviz-gh-token')
                    banner.innerHTML = `<div class="auth-banner">GitHub token expired or invalid. <a onclick="promptGitHubToken(); return false">Update your token</a> to continue.<button class="auth-dismiss" onclick="this.parentElement.parentElement.innerHTML=''">&times;</button></div>`
                } else {
                    const statusDiv = document.getElementById('json-status')
                    statusDiv.innerHTML = `<span class="error">Error loading from URL: ${error.message}</span>`
                }
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
    const textarea = document.getElementById('json-input')
    if (!textarea) return
    ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach((eventName) => {
        textarea.addEventListener(
            eventName,
            (e) => {
                e.preventDefault()
                e.stopPropagation()
            },
            false
        )
    })

    textarea.addEventListener(
        'drop',
        function (e) {
            const files = e.dataTransfer.files
            if (files.length > 0) {
                const file = files[0]
                if (file.type === 'application/json' || file.name.endsWith('.json')) {
                    const reader = new FileReader()
                    reader.onload = function (ev) {
                        try {
                            const parsed = JSON.parse(ev.target.result)
                            textarea.value = JSON.stringify(parsed, null, 2)
                        } catch (error) {
                            textarea.value = ev.target.result
                        }
                        loadJson()
                    }
                    reader.readAsText(file)
                }
            }
        },
        false
    )
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
        statusDiv.innerHTML = '<span class="error">Please select a valid JSON file</span>'
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
                statusDiv.innerHTML = '<span class="success">Valid JSON</span>'
                loadJson()
            } catch (e) {
                statusDiv.innerHTML = `<span class="error">Invalid JSON: ${e.message}</span>`
            }
        }, 500)
    })
}

export function loadJson() {
    const jsonText = document.getElementById('json-input').value.trim()
    const statusDiv = document.getElementById('json-status')

    if (!jsonText) {
        statusDiv.innerHTML = '<span class="error">Please enter JSON configuration</span>'
        return
    }

    try {
        state.currentConfig = JSON.parse(jsonText)
        clearViewStateCache()
        statusDiv.innerHTML = '<span class="success">Configuration loaded!</span>'

        renderGraph()
        renderPipelines()
        renderDatasources()
        updateFilters()
        populatePlannerSelect()
        renderStats()
    } catch (error) {
        statusDiv.innerHTML = `<span class="error">JSON Parse Error: ${error.message}</span>`
    }
}
