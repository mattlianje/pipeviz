import { state } from './state.js'

const activeFilters = {
    pipelineTags: new Set(),
    pipelineClusters: new Set(),
    datasourceTypes: new Set(),
    datasourceTags: new Set(),
    datasourceClusters: new Set()
}

export function updateFilters() {
    if (!state.currentConfig) return

    const pipelineTags = [...new Set(state.currentConfig.pipelines?.flatMap(p => p.tags || []) || [])].sort()
    const pipelineClusters = [...new Set(state.currentConfig.pipelines?.map(p => p.cluster).filter(c => c) || [])].sort()

    let pipelineHtml = ''
    if (pipelineClusters.length) {
        pipelineHtml += '<span class="filter-group"><span class="filter-group-label">cluster:</span>'
        pipelineHtml += pipelineClusters.map(c =>
            `<span class="filter-tag" data-type="pipelineClusters" data-value="${c}">${c}</span>`
        ).join('')
        pipelineHtml += '</span>'
    }
    if (pipelineTags.length) {
        pipelineHtml += '<span class="filter-group"><span class="filter-group-label">tag:</span>'
        pipelineHtml += pipelineTags.map(t =>
            `<span class="filter-tag" data-type="pipelineTags" data-value="${t}">${t}</span>`
        ).join('')
        pipelineHtml += '</span>'
    }
    document.getElementById('pipeline-filters').innerHTML = pipelineHtml

    const datasourceTypes = [...new Set(state.currentConfig.datasources?.map(ds => ds.type || 'unknown') || [])].sort()
    const datasourceTags = [...new Set(state.currentConfig.datasources?.flatMap(ds => ds.tags || []) || [])].sort()
    const datasourceClusters = [...new Set(state.currentConfig.datasources?.map(ds => ds.cluster).filter(c => c) || [])].sort()

    let datasourceHtml = ''
    if (datasourceTypes.length) {
        datasourceHtml += '<span class="filter-group"><span class="filter-group-label">type:</span>'
        datasourceHtml += datasourceTypes.map(t =>
            `<span class="filter-tag" data-type="datasourceTypes" data-value="${t}">${t}</span>`
        ).join('')
        datasourceHtml += '</span>'
    }
    if (datasourceClusters.length) {
        datasourceHtml += '<span class="filter-group"><span class="filter-group-label">cluster:</span>'
        datasourceHtml += datasourceClusters.map(c =>
            `<span class="filter-tag" data-type="datasourceClusters" data-value="${c}">${c}</span>`
        ).join('')
        datasourceHtml += '</span>'
    }
    if (datasourceTags.length) {
        datasourceHtml += '<span class="filter-group"><span class="filter-group-label">tag:</span>'
        datasourceHtml += datasourceTags.map(t =>
            `<span class="filter-tag" data-type="datasourceTags" data-value="${t}">${t}</span>`
        ).join('')
        datasourceHtml += '</span>'
    }
    document.getElementById('datasource-filters').innerHTML = datasourceHtml

    document.querySelectorAll('.filter-tag').forEach(tag => {
        tag.addEventListener('click', function() {
            const type = this.dataset.type
            const value = this.dataset.value
            if (activeFilters[type].has(value)) {
                activeFilters[type].delete(value)
                this.classList.remove('active')
            } else {
                activeFilters[type].add(value)
                this.classList.add('active')
            }
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

    rows.forEach(row => {
        const name = row.getAttribute('data-name')
        const description = row.getAttribute('data-description')
        const tags = row.getAttribute('data-tags').split(',').filter(t => t)
        const cluster = row.getAttribute('data-cluster')

        const matchesSearch = !searchTerm || name.includes(searchTerm) || description.includes(searchTerm)
        const matchesTag = activeFilters.pipelineTags.size === 0 || tags.some(t => activeFilters.pipelineTags.has(t))
        const matchesCluster = activeFilters.pipelineClusters.size === 0 || activeFilters.pipelineClusters.has(cluster)

        row.style.display = (matchesSearch && matchesTag && matchesCluster) ? '' : 'none'
    })
}

export function filterDatasources() {
    const searchTerm = document.getElementById('datasource-search').value.toLowerCase()
    const rows = document.querySelectorAll('.datasource-row')

    rows.forEach(row => {
        const searchContent = row.getAttribute('data-search')
        const type = row.getAttribute('data-type')
        const tags = row.getAttribute('data-tags').split(',').filter(t => t)
        const cluster = row.getAttribute('data-cluster')

        const matchesSearch = !searchTerm || searchContent.includes(searchTerm)
        const matchesType = activeFilters.datasourceTypes.size === 0 || activeFilters.datasourceTypes.has(type)
        const matchesTag = activeFilters.datasourceTags.size === 0 || tags.some(t => activeFilters.datasourceTags.has(t))
        const matchesCluster = activeFilters.datasourceClusters.size === 0 || activeFilters.datasourceClusters.has(cluster)

        row.style.display = (matchesSearch && matchesType && matchesTag && matchesCluster) ? '' : 'none'
    })
}
