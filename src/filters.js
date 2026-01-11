import { state } from './state.js'

const activeFilters = {
    pipelineTags: new Set(),
    pipelineClusters: new Set(),
    datasourceTypes: new Set(),
    datasourceTags: new Set(),
    datasourceClusters: new Set()
}

function buildFilterGroup(label, type, values) {
    if (values.length === 0) return ''

    const activeCount = activeFilters[type].size
    const hasActive = activeCount > 0

    let html = `<span class="filter-group" data-filter-type="${type}">`
    html += `<span class="filter-group-header${hasActive ? ' has-active' : ''}">`
    html += `<span class="filter-group-label">${label}</span>`
    html += `<span class="filter-group-count">${hasActive ? activeCount : values.length}</span>`
    html += `</span>`
    html += `<div class="filter-group-dropdown"><div class="filter-group-dropdown-inner">`
    html += values
        .map(
            (v) =>
                `<span class="filter-tag${activeFilters[type].has(v) ? ' active' : ''}" data-type="${type}" data-value="${v}">${v}</span>`
        )
        .join('')
    html += `</div></div></span>`

    return html
}

function updateFilterGroupHeaders() {
    document.querySelectorAll('.filter-group').forEach((group) => {
        const type = group.dataset.filterType
        if (!type) return

        const header = group.querySelector('.filter-group-header')
        const countEl = group.querySelector('.filter-group-count')
        const tags = group.querySelectorAll('.filter-tag')
        const activeCount = activeFilters[type].size

        if (header) {
            header.classList.toggle('has-active', activeCount > 0)
        }
        if (countEl) {
            countEl.textContent = activeCount > 0 ? activeCount : tags.length
        }
    })
}

export function updateFilters() {
    if (!state.currentConfig) return

    const pipelineTags = [...new Set(state.currentConfig.pipelines?.flatMap((p) => p.tags || []) || [])].sort()
    const pipelineClusters = [
        ...new Set(state.currentConfig.pipelines?.map((p) => p.cluster).filter((c) => c) || [])
    ].sort()

    let pipelineHtml = ''
    pipelineHtml += buildFilterGroup('cluster', 'pipelineClusters', pipelineClusters)
    pipelineHtml += buildFilterGroup('tag', 'pipelineTags', pipelineTags)
    document.getElementById('pipeline-filters').innerHTML = pipelineHtml

    const datasourceTypes = [
        ...new Set(state.currentConfig.datasources?.map((ds) => ds.type || 'unknown') || [])
    ].sort()
    const datasourceTags = [...new Set(state.currentConfig.datasources?.flatMap((ds) => ds.tags || []) || [])].sort()
    const datasourceClusters = [
        ...new Set(state.currentConfig.datasources?.map((ds) => ds.cluster).filter((c) => c) || [])
    ].sort()

    let datasourceHtml = ''
    datasourceHtml += buildFilterGroup('type', 'datasourceTypes', datasourceTypes)
    datasourceHtml += buildFilterGroup('cluster', 'datasourceClusters', datasourceClusters)
    datasourceHtml += buildFilterGroup('tag', 'datasourceTags', datasourceTags)
    document.getElementById('datasource-filters').innerHTML = datasourceHtml

    document.querySelectorAll('.filter-tag').forEach((tag) => {
        tag.addEventListener('click', function (e) {
            e.stopPropagation()
            const type = this.dataset.type
            const value = this.dataset.value
            if (activeFilters[type].has(value)) {
                activeFilters[type].delete(value)
                this.classList.remove('active')
            } else {
                activeFilters[type].add(value)
                this.classList.add('active')
            }
            updateFilterGroupHeaders()
            if (type.startsWith('pipeline')) {
                filterPipelines()
            } else {
                filterDatasources()
            }
        })
    })
}

export function filterPipelines() {
    const searchTerm = document.getElementById('pipeline-search').value.toLowerCase()
    const rows = document.querySelectorAll('.pipeline-row')

    rows.forEach((row) => {
        const name = row.getAttribute('data-name')
        const description = row.getAttribute('data-description')
        const tags = row
            .getAttribute('data-tags')
            .split(',')
            .filter((t) => t)
        const cluster = row.getAttribute('data-cluster')

        const matchesSearch = !searchTerm || name.includes(searchTerm) || description.includes(searchTerm)
        const matchesTag = activeFilters.pipelineTags.size === 0 || tags.some((t) => activeFilters.pipelineTags.has(t))
        const matchesCluster = activeFilters.pipelineClusters.size === 0 || activeFilters.pipelineClusters.has(cluster)

        row.style.display = matchesSearch && matchesTag && matchesCluster ? '' : 'none'
    })
}

export function filterDatasources() {
    const searchTerm = document.getElementById('datasource-search').value.toLowerCase()
    const rows = document.querySelectorAll('.datasource-row')

    rows.forEach((row) => {
        const searchContent = row.getAttribute('data-search')
        const type = row.getAttribute('data-type')
        const tags = row
            .getAttribute('data-tags')
            .split(',')
            .filter((t) => t)
        const cluster = row.getAttribute('data-cluster')

        const matchesSearch = !searchTerm || searchContent.includes(searchTerm)
        const matchesType = activeFilters.datasourceTypes.size === 0 || activeFilters.datasourceTypes.has(type)
        const matchesTag =
            activeFilters.datasourceTags.size === 0 || tags.some((t) => activeFilters.datasourceTags.has(t))
        const matchesCluster =
            activeFilters.datasourceClusters.size === 0 || activeFilters.datasourceClusters.has(cluster)

        row.style.display = matchesSearch && matchesType && matchesTag && matchesCluster ? '' : 'none'
    })
}
