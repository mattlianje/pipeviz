import { state } from './state.js'
import { formatSchedule } from './cron.js'
import { escapeHtml, safeUrl } from './escape.js'

export function renderPipelines() {
    if (!state.currentConfig || !state.currentConfig.pipelines) return

    const container = document.getElementById('pipelines-table-container')
    let html = `
        <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Pipeline</th>
                    <th>Description</th>
                    <th>Schedule</th>
                    <th>Input Sources</th>
                    <th>Output Sources</th>
                    <th>Cluster</th>
                    <th>Tags</th>
                    <th>Links</th>
                </tr>
            </thead>
            <tbody>
    `

    state.currentConfig.pipelines.forEach((pipeline) => {
        const inputSources =
            pipeline.input_sources?.map((s) => `<span class="badge badge-input">${escapeHtml(s)}</span>`).join('') || ''

        const outputSources =
            pipeline.output_sources?.map((s) => `<span class="badge badge-output">${escapeHtml(s)}</span>`).join('') ||
            ''

        const cluster = pipeline.cluster
            ? `<span class="badge badge-cluster">${escapeHtml(pipeline.cluster)}</span>`
            : ''

        const tags = pipeline.tags?.map((t) => `<span class="badge badge-tag">${escapeHtml(t)}</span>`).join('') || ''

        const links = pipeline.links
            ? `<div class="links-scroll">${Object.entries(pipeline.links)
                  .map(
                      ([name, url]) =>
                          `<a href="${escapeHtml(safeUrl(url))}" target="_blank" class="graph-ctrl-btn" style="font-size: 0.7em; padding: 2px 6px;">${escapeHtml(name)}</a>`
                  )
                  .join('')}</div>`
            : ''

        html += `
            <tr class="pipeline-row"
                data-name="${escapeHtml(pipeline.name.toLowerCase())}"
                data-description="${escapeHtml((pipeline.description || '').toLowerCase())}"
                data-cluster="${escapeHtml((pipeline.cluster || '').toLowerCase())}"
                data-tags="${escapeHtml((pipeline.tags || []).join(',').toLowerCase())}">
                <td class="col-name"><div><strong>${escapeHtml(pipeline.name)}</strong></div></td>
                <td class="col-desc"><div>${escapeHtml(pipeline.description || '')}</div></td>
                <td class="col-schedule"><div><code>${escapeHtml(formatSchedule(pipeline.schedule) || '')}</code></div></td>
                <td class="col-sources"><div>${inputSources}</div></td>
                <td class="col-sources"><div>${outputSources}</div></td>
                <td class="col-cluster"><div>${cluster}</div></td>
                <td class="col-tags"><div>${tags}</div></td>
                <td class="col-links"><div>${links}</div></td>
            </tr>
        `
    })

    html += '</tbody></table></div>'
    container.innerHTML = html
}

export function renderDatasources() {
    if (!state.currentConfig || !state.currentConfig.pipelines) {
        const container = document.getElementById('datasources-table-container')
        container.innerHTML = '<p class="table-empty">Load a configuration to see your data sources</p>'
        return
    }

    const allDataSources = new Map()

    if (state.currentConfig.datasources) {
        state.currentConfig.datasources.forEach((ds) => {
            allDataSources.set(ds.name, ds)
        })
    }

    state.currentConfig.pipelines.forEach((pipeline) => {
        pipeline.input_sources?.forEach((sourceName) => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, {
                    name: sourceName,
                    type: 'auto-created',
                    description: 'Auto-created from pipeline references',
                    isAutoCreated: true
                })
            }
        })
        pipeline.output_sources?.forEach((sourceName) => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, {
                    name: sourceName,
                    type: 'auto-created',
                    description: 'Auto-created from pipeline references',
                    isAutoCreated: true
                })
            }
        })
    })

    const container = document.getElementById('datasources-table-container')

    if (allDataSources.size === 0) {
        container.innerHTML = '<p class="table-empty">No data sources found</p>'
        return
    }

    let html = `
        <div class="table-container">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Name & Type</th>
                    <th>Description</th>
                    <th>Owner</th>
                    <th>Metadata</th>
                    <th>Cluster</th>
                    <th>Tags</th>
                    <th>Links</th>
                </tr>
            </thead>
            <tbody>
    `

    allDataSources.forEach((ds) => {
        const typeBadge = `<span class="badge badge-${escapeHtml(ds.type || 'secondary')}">${escapeHtml((ds.type || 'unknown').toUpperCase())}</span>`

        const metadata = ds.metadata
            ? Object.entries(ds.metadata)
                  .map(
                      ([k, v]) =>
                          `<div style="font-size: 11px; color: var(--text-muted); margin-bottom: 2px;"><span style="font-weight: 500;">${escapeHtml(k.replace(/_/g, ' '))}:</span> ${escapeHtml(v)}</div>`
                  )
                  .join('')
            : ''

        const cluster = ds.cluster ? `<span class="badge badge-cluster">${escapeHtml(ds.cluster)}</span>` : ''

        const tags = ds.tags?.map((t) => `<span class="badge badge-tag">${escapeHtml(t)}</span>`).join('') || ''

        const links = ds.links
            ? `<div class="links-scroll">${Object.entries(ds.links)
                  .map(
                      ([name, url]) =>
                          `<a href="${escapeHtml(safeUrl(url))}" target="_blank" class="graph-ctrl-btn" style="font-size: 0.7em; padding: 2px 6px;">${escapeHtml(name)}</a>`
                  )
                  .join('')}</div>`
            : ''

        html += `
            <tr class="datasource-row"
                data-name="${escapeHtml(ds.name.toLowerCase())}"
                data-type="${escapeHtml((ds.type || '').toLowerCase())}"
                data-cluster="${escapeHtml((ds.cluster || '').toLowerCase())}"
                data-tags="${escapeHtml((ds.tags || []).join(',').toLowerCase())}"
                data-search="${escapeHtml((ds.name + ' ' + (ds.description || '') + ' ' + (ds.owner || '')).toLowerCase())}">
                <td class="col-name"><div><strong>${escapeHtml(ds.name)}</strong><br>${typeBadge}</div></td>
                <td class="col-desc"><div>${ds.isAutoCreated ? `<span style="color: var(--text-secondary); font-style: italic;">${escapeHtml(ds.description)}</span>` : escapeHtml(ds.description || '')}</div></td>
                <td class="col-owner"><div style="font-size: 11px; color: var(--text-muted);">${escapeHtml(ds.owner || '')}</div></td>
                <td class="col-metadata"><div>${metadata}</div></td>
                <td class="col-cluster"><div>${cluster}</div></td>
                <td class="col-tags"><div>${tags}</div></td>
                <td class="col-links"><div>${links}</div></td>
            </tr>
        `
    })

    html += '</tbody></table></div>'
    container.innerHTML = html
}
