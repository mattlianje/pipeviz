import { state } from './state.js'
import { formatSchedule } from './cron.js'

export function renderPipelines() {
    if (!state.currentConfig || !state.currentConfig.pipelines) return

    const container = document.getElementById('pipelines-table-container')
    let html = `
        <div class="table-container">
        <table class="table table-striped table-hover">
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
            pipeline.input_sources
                ?.map(
                    (s) =>
                        `<span class="badge me-1 mb-1" style="background-color: #e3f2fd; color: #1565c0; font-size: 0.85em;">${s}</span>`
                )
                .join('') || ''

        const outputSources =
            pipeline.output_sources
                ?.map(
                    (s) =>
                        `<span class="badge me-1 mb-1" style="background-color: #e8f5e8; color: #2e7d32; font-size: 0.85em;">${s}</span>`
                )
                .join('') || ''

        const cluster = pipeline.cluster ? `<span class="badge badge-cluster me-1 mb-1">${pipeline.cluster}</span>` : ''

        const tags =
            pipeline.tags
                ?.map(
                    (t) =>
                        `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404; font-size: 0.85em;">${t}</span>`
                )
                .join('') || ''

        const links = pipeline.links
            ? `<div class="links-scroll">${Object.entries(pipeline.links)
                  .map(
                      ([name, url]) =>
                          `<a href="${url}" target="_blank" class="graph-ctrl-btn" style="font-size: 0.7em; padding: 2px 6px;">${name}</a>`
                  )
                  .join('')}</div>`
            : ''

        html += `
            <tr class="pipeline-row"
                data-name="${pipeline.name.toLowerCase()}"
                data-description="${(pipeline.description || '').toLowerCase()}"
                data-cluster="${(pipeline.cluster || '').toLowerCase()}"
                data-tags="${(pipeline.tags || []).join(',').toLowerCase()}">
                <td class="col-name"><div><strong>${pipeline.name}</strong></div></td>
                <td class="col-desc"><div>${pipeline.description || ''}</div></td>
                <td class="col-schedule"><div><code class="text-success">${formatSchedule(pipeline.schedule) || ''}</code></div></td>
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
        container.innerHTML = '<p class="text-muted text-center">Load a configuration to see your data sources</p>'
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
        container.innerHTML = '<p class="text-muted text-center">No data sources found</p>'
        return
    }

    let html = `
        <div class="table-container">
        <table class="table table-striped table-hover">
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
        const typeBadge = `<span class="badge badge-${ds.type || 'secondary'}">${(ds.type || 'unknown').toUpperCase()}</span>`

        const metadata = ds.metadata
            ? Object.entries(ds.metadata)
                  .map(
                      ([k, v]) =>
                          `<div class="small text-muted mb-1"><span style="font-weight: 500;">${k.replace(/_/g, ' ')}:</span> ${v}</div>`
                  )
                  .join('')
            : ''

        const cluster = ds.cluster ? `<span class="badge badge-cluster me-1 mb-1">${ds.cluster}</span>` : ''

        const tags =
            ds.tags
                ?.map(
                    (t) =>
                        `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404; font-size: 0.85em;">${t}</span>`
                )
                .join('') || ''

        const links = ds.links
            ? `<div class="links-scroll">${Object.entries(ds.links)
                  .map(
                      ([name, url]) =>
                          `<a href="${url}" target="_blank" class="graph-ctrl-btn" style="font-size: 0.7em; padding: 2px 6px;">${name}</a>`
                  )
                  .join('')}</div>`
            : ''

        html += `
            <tr class="datasource-row"
                data-name="${ds.name.toLowerCase()}"
                data-type="${(ds.type || '').toLowerCase()}"
                data-cluster="${(ds.cluster || '').toLowerCase()}"
                data-tags="${(ds.tags || []).join(',').toLowerCase()}"
                data-search="${(ds.name + ' ' + (ds.description || '') + ' ' + (ds.owner || '')).toLowerCase()}">
                <td class="col-name"><div><strong>${ds.name}</strong><br>${typeBadge}</div></td>
                <td class="col-desc"><div>${ds.isAutoCreated ? `<span class="text-secondary fst-italic">${ds.description}</span>` : ds.description || ''}</div></td>
                <td class="col-owner"><div class="small text-muted">${ds.owner || ''}</div></td>
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
