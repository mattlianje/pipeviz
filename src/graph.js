import { state, getConfigHash } from './state.js'
import { renderAttributeGraph } from './attributes.js'

export function generateGraphvizDot() {
    if (!state.currentConfig) return '';

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const edgeColor = isDark ? '#b0b0b0' : '#555';

    let pipelines = state.currentConfig.pipelines || [];
    const datasources = state.currentConfig.datasources || [];
    const explicitClusters = state.currentConfig.clusters || [];

    // Handle grouped view - aggregate pipelines by group (unless expanded)
    if (state.groupedView) {
        const groups = new Map();
        const ungroupedPipelines = [];

        pipelines.forEach(p => {
            if (p.group && !state.expandedGroups.has(p.group)) {
                // Only group if not expanded
                if (!groups.has(p.group)) {
                    groups.set(p.group, {
                        name: p.group,
                        description: `Grouped pipelines: ${p.group}`,
                        input_sources: new Set(),
                        output_sources: new Set(),
                        cluster: p.cluster,
                        _isGroup: true,
                        _members: []
                    });
                }
                const group = groups.get(p.group);
                p.input_sources?.forEach(s => group.input_sources.add(s));
                p.output_sources?.forEach(s => group.output_sources.add(s));
                group._members.push(p.name);
            } else {
                ungroupedPipelines.push(p);
            }
        });

        // Convert groups to pipeline-like objects
        const groupedPipelines = Array.from(groups.values()).map(g => ({
            ...g,
            input_sources: Array.from(g.input_sources),
            output_sources: Array.from(g.output_sources)
        }));

        pipelines = [...ungroupedPipelines, ...groupedPipelines];
    }

    // Collect ALL data sources (explicit + auto-created from pipeline references)
    const allDataSources = new Map();
    
    // Add explicit data sources
    datasources.forEach(ds => {
        allDataSources.set(ds.name, ds);
    });
    
    // Add auto-created data sources from pipeline input/output references
    pipelines.forEach(pipeline => {
        pipeline.input_sources?.forEach(sourceName => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, { 
                    name: sourceName, 
                    type: 'auto-created'
                    // No cluster assignment - let auto-created sources be unclustered
                });
            }
        });
        pipeline.output_sources?.forEach(sourceName => {
            if (!allDataSources.has(sourceName)) {
                allDataSources.set(sourceName, { 
                    name: sourceName, 
                    type: 'auto-created'
                    // No cluster assignment - let auto-created sources be unclustered
                });
            }
        });
    });

    // Collect all clusters mentioned in pipelines and datasources
    const allClusterNames = new Set();
    pipelines.forEach(p => { if (p.cluster) allClusterNames.add(p.cluster); });
    allDataSources.forEach(ds => { if (ds.cluster) allClusterNames.add(ds.cluster); });

    // Build cluster hierarchy
    const clusterDefinitions = new Map();
    const clusterHierarchy = new Map(); // child -> parent
    const clusterChildren = new Map(); // parent -> [children]

    // Process explicit clusters
    explicitClusters.forEach(cluster => {
        clusterDefinitions.set(cluster.name, cluster);
        allClusterNames.add(cluster.name);

        if (cluster.parent) {
            clusterHierarchy.set(cluster.name, cluster.parent);
            if (!clusterChildren.has(cluster.parent)) {
                clusterChildren.set(cluster.parent, []);
            }
            clusterChildren.get(cluster.parent).push(cluster.name);
            allClusterNames.add(cluster.parent); // Ensure parent exists
        }
    });

    // Add implicit clusters (those referenced but not explicitly defined)
    allClusterNames.forEach(name => {
        if (!clusterDefinitions.has(name)) {
            clusterDefinitions.set(name, { name, description: `Auto-generated cluster: ${name}` });
        }
    });

    let dot = `digraph PipevizGraph {
    rankdir=LR;
    bgcolor="transparent";
    node [fontsize=12];
    edge [fontsize=10];

`;

    // Generate cluster colors automatically
    const clusterColors = [
        "#1976d2", "#7b1fa2", "#388e3c", "#f57c00", "#d32f2f",
        "#1976d2", "#616161", "#c2185b", "#303f9f", "#f57c00"
    ];

    // Group nodes by cluster
    const nodesByCluster = new Map();

    pipelines.forEach(pipeline => {
        const cluster = pipeline.cluster || '_unclustered';
        if (!nodesByCluster.has(cluster)) {
            nodesByCluster.set(cluster, []);
        }
        nodesByCluster.get(cluster).push({ type: 'pipeline', node: pipeline });
    });

    // Use ALL data sources (explicit + auto-created)
    allDataSources.forEach(ds => {
        const cluster = ds.cluster || '_unclustered';
        if (!nodesByCluster.has(cluster)) {
            nodesByCluster.set(cluster, []);
        }
        nodesByCluster.get(cluster).push({ type: 'datasource', node: ds });
    });

    // Find root clusters (those without parents)
    const rootClusters = [];
    clusterDefinitions.forEach((cluster, name) => {
        if (!clusterHierarchy.has(name) && nodesByCluster.has(name)) {
            rootClusters.push(name);
        }
    });

    let colorIndex = 0;

    function renderCluster(clusterName, depth = 0) {
        const cluster = clusterDefinitions.get(clusterName);
        if (!cluster) return '';

        const nodesInCluster = nodesByCluster.get(clusterName) || [];
        const children = clusterChildren.get(clusterName) || [];

        // Skip if cluster has no nodes and no children with nodes
        const hasNodes = nodesInCluster.length > 0;
        const hasChildrenWithNodes = children.some(child =>
            nodesByCluster.has(child) && nodesByCluster.get(child).length > 0
        );

        if (!hasNodes && !hasChildrenWithNodes) return '';

        const clusterColor = clusterColors[colorIndex % clusterColors.length];
        colorIndex++;

        let result = `${'    '.repeat(depth + 1)}subgraph cluster_${clusterName.replace(/[^a-zA-Z0-9]/g, '_')} {
${'    '.repeat(depth + 2)}label="${clusterName}";
${'    '.repeat(depth + 2)}style="dotted";
${'    '.repeat(depth + 2)}color="#666666";
${'    '.repeat(depth + 2)}fontsize=11;
${'    '.repeat(depth + 2)}fontname="Arial";

`;

        // Add nodes in this cluster
        nodesInCluster.forEach(item => {
            if (item.type === 'pipeline') {
                const pipeline = item.node;
                const isGroup = pipeline._isGroup;
                const memberCount = pipeline._members?.length || 0;
                const label = isGroup ?
                    `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>` :
                    (pipeline.schedule ?
                        `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${pipeline.schedule}</I></FONT>>` :
                        `"${pipeline.name}"`);
                const fillColor = isGroup ? '#e0f2f1' : '#e3f2fd';
                const borderColor = isGroup ? '#00897b' : '#1976d2';
                const penWidth = isGroup ? '2' : '1';
                result += `${'    '.repeat(depth + 2)}"${pipeline.name}" [shape=box, style="filled,rounded",
${'    '.repeat(depth + 3)}fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
${'    '.repeat(depth + 3)}fontname="Arial",
${'    '.repeat(depth + 3)}label=${label}];
`;
            } else if (item.type === 'datasource') {
                const ds = item.node;
                result += `${'    '.repeat(depth + 2)}"${ds.name}" [shape=ellipse, style=filled,
${'    '.repeat(depth + 3)}fillcolor="#f3e5f5", color="#7b1fa2",
${'    '.repeat(depth + 3)}fontname="Arial", fontsize=10];
`;
            }
        });

        // Add child clusters
        children.forEach(childName => {
            result += renderCluster(childName, depth + 1);
        });

        result += `${'    '.repeat(depth + 1)}}

`;
        return result;
    }

    // Render all root clusters
    rootClusters.forEach(clusterName => {
        dot += renderCluster(clusterName);
    });

    // Add unclustered nodes
    const unclusteredNodes = nodesByCluster.get('_unclustered') || [];
    unclusteredNodes.forEach(item => {
        if (item.type === 'pipeline') {
            const pipeline = item.node;
            const isGroup = pipeline._isGroup;
            const memberCount = pipeline._members?.length || 0;
            const label = isGroup ?
                `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#666">(${memberCount} pipelines)</FONT>>` :
                (pipeline.schedule ?
                    `<${pipeline.name}<BR/><FONT POINT-SIZE="9" COLOR="#d63384"><I>${pipeline.schedule}</I></FONT>>` :
                    `"${pipeline.name}"`);
            const fillColor = isGroup ? '#e0f2f1' : '#e3f2fd';
            const borderColor = isGroup ? '#00897b' : '#1976d2';
            const penWidth = isGroup ? '2' : '1';
            dot += `    "${pipeline.name}" [shape=box, style="filled,rounded",
        fillcolor="${fillColor}", color="${borderColor}", penwidth=${penWidth},
        fontname="Arial",
        label=${label}];
`;
        } else if (item.type === 'datasource') {
            const ds = item.node;
            dot += `    "${ds.name}" [shape=ellipse, style=filled,
        fillcolor="#f3e5f5", color="#7b1fa2",
        fontname="Arial", fontsize=10];
`;
        }
    });

    // Add data flow edges
    dot += '\n';
    pipelines.forEach(pipeline => {
        pipeline.input_sources?.forEach(source => {
            dot += `    "${source}" -> "${pipeline.name}" [color="${edgeColor}", arrowsize=0.8];\n`;
        });
        pipeline.output_sources?.forEach(source => {
            dot += `    "${pipeline.name}" -> "${source}" [color="${edgeColor}", arrowsize=0.8];\n`;
        });
    });

    // Add pipeline dependencies
    dot += '\n';
    pipelines.forEach(pipeline => {
        pipeline.upstream_pipelines?.forEach(upstream => {
            dot += `    "${upstream}" -> "${pipeline.name}" [color="#ff6b35", style="solid", arrowsize=0.8];\n`;
        });
    });

    dot += '\n    overlap=false; splines=true;\n}';
    return dot;
}



let graphTabListenerAdded = false;

export function renderGraph() {
    if (!state.currentConfig) return;

    const currentHash = getConfigHash(state.currentConfig);

    if (!graphTabListenerAdded) {
        document.getElementById('graph-tab').addEventListener('shown.bs.tab', function() {
            const newHash = getConfigHash(state.currentConfig);
            if (!state.graphviz) {
                setTimeout(initializeGraph, 100);
                state.lastRenderedConfigHash = newHash;
            } else if (newHash !== state.lastRenderedConfigHash) {
                // Only re-render if config has changed
                updateGraph();
                state.lastRenderedConfigHash = newHash;
            }
        });
        graphTabListenerAdded = true;
    }

    if (document.getElementById('graph-tab').classList.contains('active')) {
        setTimeout(initializeGraph, 100);
        state.lastRenderedConfigHash = currentHash;
    }
}

export function initializeGraph() {
    try {
        // Clear loading indicator
        document.getElementById('graph').innerHTML = '';
        state.graphviz = d3.select("#graph").graphviz()
            .width(document.getElementById('graph').clientWidth)
            .height(500)
            .fit(true)
            .on("initEnd", setupGraphInteractivity)
            .on("renderEnd", setupGraphInteractivity);
        updateGraph();
    } catch (error) {
        console.error('Graphviz initialization error:', error);
        // Fallback: show DOT code in graph area
        document.getElementById('graph').innerHTML = `
            <div class="alert alert-warning m-3">
                <strong>Graph rendering issue detected.</strong><br>
                Please check the Raw DOT tab to see the generated code.<br>
                <small>Error: ${error.message}</small>
            </div>
        `;
    }
}

export function updateGraph() {
    if (state.graphviz) {
        const dotSrc = generateGraphvizDot();
        state.graphviz.renderDot(dotSrc);
    }
}

export function setupGraphInteractivity() {
    // Build and cache lineage maps once on render
    state.cachedUpstreamMap = {};
    state.cachedDownstreamMap = {};
    state.cachedLineage = {};

    d3.select("#graph").selectAll(".edge").each(function() {
        const edge = d3.select(this);
        const title = edge.select("title").text();
        const match = title.match(/^(.+?)(?:->|--)\s*(.+?)$/);
        if (match) {
            const source = match[1];
            const target = match[2];
            if (!state.cachedDownstreamMap[source]) state.cachedDownstreamMap[source] = [];
            if (!state.cachedUpstreamMap[target]) state.cachedUpstreamMap[target] = [];
            state.cachedDownstreamMap[source].push(target);
            state.cachedUpstreamMap[target].push(source);
        }
    });

    // Pre-compute full lineage for all nodes with distance
    function getFullChain(node, map, visited = new Set(), depth = 1) {
        if (visited.has(node)) return [];
        visited.add(node);
        const neighbors = map[node] || [];
        let result = [];
        neighbors.forEach(n => {
            result.push({ name: n, depth: depth });
            result.push(...getFullChain(n, map, visited, depth + 1));
        });
        return result;
    }

    d3.select("#graph").selectAll(".node").each(function() {
        const nodeName = d3.select(this).select("title").text();
        state.cachedLineage[nodeName] = {
            upstream: getFullChain(nodeName, state.cachedUpstreamMap, new Set()),
            downstream: getFullChain(nodeName, state.cachedDownstreamMap, new Set())
        };
    });

    if (document.getElementById('node-tooltip')) {
        document.getElementById('node-tooltip').remove();
    }
    const tooltip = document.createElement('div');
    tooltip.id = 'node-tooltip';
    tooltip.className = 'node-tooltip';
    tooltip.style.display = 'none';
    document.body.appendChild(tooltip);

    d3.select("#graph").selectAll(".node")
        .style("cursor", "pointer")
        .on("click", function(event, d) {
            event.stopPropagation();
            const nodeName = d3.select(this).select("title").text();
            selectNode(nodeName, this);
        })
        .on("dblclick", function(event, d) {
            event.stopPropagation();
            const nodeName = d3.select(this).select("title").text();
            // Check if this is a group node
            if (state.groupedView && state.currentConfig?.pipelines) {
                const isGroupNode = state.currentConfig.pipelines.some(p => p.group === nodeName);
                if (isGroupNode) {
                    toggleGroup(nodeName);
                }
            }
        })
        .on("mouseover", function(event, d) {
            const nodeName = d3.select(this).select("title").text();
            showNodeTooltip(event, nodeName);
        })
        .on("mousemove", function(event, d) {
            tooltip.style.left = (event.pageX + 10) + 'px';
            tooltip.style.top = (event.pageY - 10) + 'px';
        })
        .on("mouseout", function(event, d) {
            hideNodeTooltip();
        });

    d3.select("#graph").on("click", function(event) {
        // Check if clicked on background
        if (event.target.tagName === "svg" || event.target === event.currentTarget || 
            event.target.classList.contains('graph-container') || 
            !event.target.closest('.node')) {
            clearSelection();
        }
    });
}

export function showNodeTooltip(event, nodeName) {
    const tooltip = document.getElementById('node-tooltip');
    if (!tooltip || !state.currentConfig) return;

    let content = '';

    // Check if this is a group node
    if (state.groupedView) {
        const groupMembers = state.currentConfig.pipelines?.filter(p => p.group === nodeName);
        if (groupMembers?.length > 0) {
            content = `${nodeName} (${groupMembers.length} pipelines)`;
            content += `\nClick for details`;
            content += `\nMembers: ${groupMembers.map(p => p.name).join(', ')}`;
            tooltip.textContent = content;
            tooltip.style.display = 'block';
            tooltip.style.left = (event.pageX + 10) + 'px';
            tooltip.style.top = (event.pageY - 10) + 'px';
            return;
        }
    }

    const pipeline = state.currentConfig.pipelines?.find(p => p.name === nodeName);
    if (pipeline) {
        content = `${pipeline.name}`;
        if (pipeline.description) content += `\nDescription: ${pipeline.description}`;
        if (pipeline.schedule) content += `\nSchedule: ${pipeline.schedule}`;
        if (pipeline.cluster) content += `\nCluster: ${pipeline.cluster}`;
        if (pipeline.tags && pipeline.tags.length > 0) content += `\nTags: ${pipeline.tags.join(', ')}`;
        if (pipeline.input_sources && pipeline.input_sources.length > 0) content += `\nInputs: ${pipeline.input_sources.join(', ')}`;
        if (pipeline.output_sources && pipeline.output_sources.length > 0) content += `\nOutputs: ${pipeline.output_sources.join(', ')}`;
    }

    const datasource = state.currentConfig.datasources?.find(ds => ds.name === nodeName);
    if (datasource) {
        content = `${datasource.name}`;
        if (datasource.type) content += `\nType: ${datasource.type.toUpperCase()}`;
        if (datasource.description) content += `\nDescription: ${datasource.description}`;
        if (datasource.owner) content += `\nOwner: ${datasource.owner}`;
        if (datasource.cluster) content += `\nCluster: ${datasource.cluster}`;
        if (datasource.tags && datasource.tags.length > 0) content += `\nTags: ${datasource.tags.join(', ')}`;

        if (datasource.metadata) {
            const keyFields = ['size', 'record_count', 'refresh_frequency', 'environment'];
            keyFields.forEach(field => {
                if (datasource.metadata[field]) {
                    content += `\n${field.replace(/_/g, ' ')}: ${datasource.metadata[field]}`;
                }
            });
        }
    }

    if (content) {
        tooltip.textContent = content;
        tooltip.style.display = 'block';
        tooltip.style.left = (event.pageX + 10) + 'px';
        tooltip.style.top = (event.pageY - 10) + 'px';
    }
}

export function hideNodeTooltip() {
    const tooltip = document.getElementById('node-tooltip');
    if (tooltip) {
        tooltip.style.display = 'none';
    }
}

export function selectNode(nodeName, nodeElement) {
    state.selectedNode = nodeName;
    clearHighlights();
    d3.select(nodeElement).classed("node-highlighted", true);

    // Use cached lineage for performance
    const lineage = state.cachedLineage[nodeName] || { upstream: [], downstream: [] };
    const upstream = lineage.upstream;
    const downstream = lineage.downstream;
    const allConnected = new Set([...upstream.map(x => x.name), ...downstream.map(x => x.name)]);

    // Highlight connected nodes
    d3.select("#graph").selectAll(".node").each(function() {
        const node = d3.select(this);
        const nodeTitle = node.select("title").text();
        if (allConnected.has(nodeTitle)) {
            node.classed("node-connected", true);
        } else if (nodeTitle !== nodeName) {
            node.classed("node-dimmed", true);
        }
    });

    // Highlight edges in the chain
    d3.select("#graph").selectAll(".edge").each(function() {
        const edge = d3.select(this);
        const title = edge.select("title").text();
        const match = title.match(/^(.+?)(?:->|--)\s*(.+?)$/);
        if (match) {
            const source = match[1];
            const target = match[2];
            const sourceInChain = source === nodeName || allConnected.has(source);
            const targetInChain = target === nodeName || allConnected.has(target);
            if (sourceInChain && targetInChain) {
                edge.classed("edge-highlighted", true);
            } else {
                edge.classed("edge-dimmed", true);
            }
        }
    });

    // Show node details in side panel
    showNodeDetails(nodeName, upstream, downstream);
}

export function showNodeDetails(nodeName, upstream = [], downstream = []) {
    const col = document.getElementById('node-details-col');
    const content = document.getElementById('node-details-content');

    if (!state.currentConfig) return;

    let html = '';
    let nodeData = null;
    let nodeType = '';

    // Check if it's a pipeline
    const pipeline = state.currentConfig.pipelines?.find(p => p.name === nodeName);
    if (pipeline) {
        nodeData = pipeline;
        nodeType = 'Pipeline';
    }

    // Check if it's a pipeline group (in grouped view mode)
    if (!nodeData && state.groupedView) {
        const groupMembers = state.currentConfig.pipelines?.filter(p => p.group === nodeName);
        if (groupMembers?.length > 0) {
            const allInputs = new Set();
            const allOutputs = new Set();
            groupMembers.forEach(p => {
                p.input_sources?.forEach(s => allInputs.add(s));
                p.output_sources?.forEach(s => allOutputs.add(s));
            });
            nodeData = {
                name: nodeName,
                description: `Group containing ${groupMembers.length} pipelines`,
                input_sources: Array.from(allInputs),
                output_sources: Array.from(allOutputs),
                cluster: groupMembers[0].cluster,
                _members: groupMembers.map(p => p.name)
            };
            nodeType = 'Pipeline Group';
        }
    }

    // Check if it's a datasource
    const datasource = state.currentConfig.datasources?.find(ds => ds.name === nodeName);
    if (datasource) {
        nodeData = datasource;
        nodeType = 'Data Source';
    }

    // Check auto-created datasources
    if (!nodeData) {
        const allSources = new Set();
        state.currentConfig.pipelines?.forEach(p => {
            p.input_sources?.forEach(s => allSources.add(s));
            p.output_sources?.forEach(s => allSources.add(s));
        });
        if (allSources.has(nodeName)) {
            nodeData = { name: nodeName, description: 'Auto-created from pipeline references' };
            nodeType = 'Data Source';
        }
    }

    if (!nodeData) {
        col.style.display = 'none';
        return;
    }

    html = `<h5>${nodeData.name}</h5>`;
    html += `<div class="detail-label">Type</div>`;
    html += `<div class="detail-value"><span class="badge bg-secondary">${nodeType}</span></div>`;

    // Add expand/collapse button for pipeline groups
    if (nodeType === 'Pipeline Group') {
        const isExpanded = state.expandedGroups.has(nodeName);
        html += `<div class="mt-2 mb-2">
            <button class="btn btn-sm btn-outline-warning" onclick="toggleGroup('${nodeName}')">
                ${isExpanded ? 'Collapse Group' : 'Expand Group'}
            </button>
        </div>`;
    }

    // Add collapse button for pipelines that belong to an expanded group
    if (nodeType === 'Pipeline' && nodeData.group && state.expandedGroups.has(nodeData.group)) {
        html += `<div class="mt-2 mb-2">
            <button class="btn btn-sm btn-outline-secondary" onclick="toggleGroup('${nodeData.group}')">
                Collapse Group (${nodeData.group})
            </button>
        </div>`;
    }

    if (nodeData.description) {
        html += `<div class="detail-label">Description</div>`;
        html += `<div class="detail-value">${nodeData.description}</div>`;
    }

    if (nodeData.schedule) {
        html += `<div class="detail-label">Schedule</div>`;
        html += `<div class="detail-value"><code class="text-success">${nodeData.schedule}</code></div>`;
    }

    if (nodeData.type) {
        html += `<div class="detail-label">Source Type</div>`;
        html += `<div class="detail-value"><span class="badge badge-${nodeData.type}">${nodeData.type.toUpperCase()}</span></div>`;
    }

    if (nodeData.owner) {
        html += `<div class="detail-label">Owner</div>`;
        html += `<div class="detail-value">${nodeData.owner}</div>`;
    }

    if (nodeData.cluster) {
        html += `<div class="detail-label">Cluster</div>`;
        html += `<div class="detail-value"><span class="badge badge-cluster">${nodeData.cluster}</span></div>`;
    }

    if (nodeData.input_sources?.length) {
        html += `<div class="detail-label">Input Sources</div>`;
        html += `<div class="detail-value">${nodeData.input_sources.map(s =>
            `<span class="badge me-1 mb-1" style="background-color: #e3f2fd; color: #1565c0;">${s}</span>`
        ).join('')}</div>`;
    }

    if (nodeData.output_sources?.length) {
        html += `<div class="detail-label">Output Sources</div>`;
        html += `<div class="detail-value">${nodeData.output_sources.map(s =>
            `<span class="badge me-1 mb-1" style="background-color: #e8f5e8; color: #2e7d32;">${s}</span>`
        ).join('')}</div>`;
    }

    if (nodeData.upstream_pipelines?.length) {
        html += `<div class="detail-label">Upstream Pipelines</div>`;
        html += `<div class="detail-value">${nodeData.upstream_pipelines.map(p =>
            `<span class="badge me-1 mb-1" style="background-color: #fff3e0; color: #e65100;">${p}</span>`
        ).join('')}</div>`;
    }

    if (nodeData._members?.length) {
        html += `<div class="detail-label">Member Pipelines</div>`;
        html += `<div class="detail-value">${nodeData._members.map(p =>
            `<span class="badge me-1 mb-1" style="background-color: #e0f2f1; color: #00897b;">${p}</span>`
        ).join('')}</div>`;
    }

    if (nodeData.tags?.length) {
        html += `<div class="detail-label">Tags</div>`;
        html += `<div class="detail-value">${nodeData.tags.map(t =>
            `<span class="badge me-1 mb-1" style="background-color: #fff3cd; color: #856404;">${t}</span>`
        ).join('')}</div>`;
    }

    if (nodeData.metadata && Object.keys(nodeData.metadata).length) {
        html += `<div class="detail-label">Metadata</div>`;
        html += `<div class="detail-value">`;
        Object.entries(nodeData.metadata).forEach(([key, value]) => {
            html += `<div class="small"><strong>${key.replace(/_/g, ' ')}:</strong> ${value}</div>`;
        });
        html += `</div>`;
    }

    if (nodeData.links && Object.keys(nodeData.links).length) {
        html += `<div class="links-section">`;
        html += `<div class="detail-label">Links</div>`;
        Object.entries(nodeData.links).forEach(([name, url]) => {
            html += `<a href="${url}" target="_blank" class="btn btn-sm btn-outline-primary link-btn">${name}</a>`;
        });
        html += `</div>`;
    }

    // Categorize upstream/downstream by type, deduplicate, sort by depth
    const pipelineNames = new Set((state.currentConfig.pipelines || []).map(p => p.name));
    if (state.groupedView) {
        (state.currentConfig.pipelines || []).forEach(p => {
            if (p.group) pipelineNames.add(p.group);
        });
    }

    // Deduplicate keeping lowest depth, then sort by depth
    function dedupeAndSort(items, filterFn) {
        const seen = new Map();
        items.filter(x => filterFn(x.name)).forEach(x => {
            if (!seen.has(x.name) || seen.get(x.name).depth > x.depth) {
                seen.set(x.name, x);
            }
        });
        return [...seen.values()].sort((a, b) => a.depth - b.depth);
    }

    const upstreamPipelines = dedupeAndSort(upstream, n => pipelineNames.has(n));
    const upstreamSources = dedupeAndSort(upstream, n => !pipelineNames.has(n));
    const downstreamPipelines = dedupeAndSort(downstream, n => pipelineNames.has(n));
    const downstreamSources = dedupeAndSort(downstream, n => !pipelineNames.has(n));

    // Render with depth indication
    function renderLineageList(items, label) {
        if (items.length === 0) return '';
        let out = `<div class="detail-label">${label} (${items.length})</div><div class="detail-value">`;
        items.forEach(x => {
            const indent = (x.depth - 1) * 12;
            const opacity = Math.max(0.5, 1 - (x.depth - 1) * 0.15);
            const prefix = x.depth > 1 ? '└ ' : '';
            out += `<div class="lineage-link" data-node-name="${x.name}" style="padding-left: ${indent}px; opacity: ${opacity};">${prefix}${x.name}</div>`;
        });
        out += `</div>`;
        return out;
    }

    html += renderLineageList(upstreamPipelines, 'UPSTREAM PIPELINES');
    html += renderLineageList(upstreamSources, 'UPSTREAM SOURCES');
    html += renderLineageList(downstreamPipelines, 'DOWNSTREAM PIPELINES');
    html += renderLineageList(downstreamSources, 'DOWNSTREAM SOURCES');

    // Shrink graph and show panel
    const graphCol = document.getElementById('graph-col');
    graphCol.classList.remove('col-md-12');
    graphCol.classList.add('col-md-8');
    col.style.display = 'block';
    content.innerHTML = html;

    // Add click handlers for lineage links
    content.querySelectorAll('.lineage-link').forEach(el => {
        el.addEventListener('click', function() {
            const targetName = this.getAttribute('data-node-name');
            if (targetName) {
                // Find and click the node in the graph
                d3.select("#graph").selectAll(".node").each(function() {
                    const nodeTitle = d3.select(this).select("title").text();
                    if (nodeTitle === targetName) {
                        selectNode(targetName, this);
                    }
                });
            }
        });
    });
}

export function clearSelection() {
    state.selectedNode = null;
    clearHighlights();
    // Hide details panel and expand graph
    const col = document.getElementById('node-details-col');
    const graphCol = document.getElementById('graph-col');
    if (col) col.style.display = 'none';
    if (graphCol) {
        graphCol.classList.remove('col-md-8');
        graphCol.classList.add('col-md-12');
    }
}

export function clearHighlights() {
    d3.select("#graph").selectAll(".node")
        .classed("node-highlighted node-connected node-dimmed", false);
    d3.select("#graph").selectAll(".edge")
        .classed("edge-highlighted edge-dimmed", false);
}

export function fuzzyMatch(text, query) {
    text = text.toLowerCase();
    query = query.toLowerCase();

    // Direct substring match
    if (text.includes(query)) {
        return { match: true, score: query.length / text.length + 0.5 };
    }

    // Fuzzy match - all query chars must appear in order
    let queryIdx = 0;
    let score = 0;
    let lastMatchIdx = -1;

    for (let i = 0; i < text.length && queryIdx < query.length; i++) {
        if (text[i] === query[queryIdx]) {
            score += 1;
            // Bonus for consecutive matches
            if (lastMatchIdx === i - 1) score += 0.5;
            // Bonus for matching at word boundaries
            if (i === 0 || text[i-1] === '_' || text[i-1] === '-' || text[i-1] === ' ') score += 0.3;
            lastMatchIdx = i;
            queryIdx++;
        }
    }

    if (queryIdx === query.length) {
        return { match: true, score: score / text.length };
    }
    return { match: false, score: 0 };
}

export function highlightMatch(text, query) {
    const lowerText = text.toLowerCase();
    const lowerQuery = query.toLowerCase();

    // Try substring match first
    const idx = lowerText.indexOf(lowerQuery);
    if (idx !== -1) {
        return text.substring(0, idx) +
               '<span class="result-match">' + text.substring(idx, idx + query.length) + '</span>' +
               text.substring(idx + query.length);
    }

    // Fuzzy highlight
    let result = '';
    let queryIdx = 0;
    for (let i = 0; i < text.length; i++) {
        if (queryIdx < query.length && text[i].toLowerCase() === lowerQuery[queryIdx]) {
            result += '<span class="result-match">' + text[i] + '</span>';
            queryIdx++;
        } else {
            result += text[i];
        }
    }
    return result;
}

export function searchNodes(event) {
    const dropdown = document.getElementById('graph-search-results');
    const items = dropdown.querySelectorAll('.search-result-item[data-name]');

    // Handle arrow key navigation
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        if (items.length === 0) return;

        const current = dropdown.querySelector('.search-result-item.selected');
        let index = current ? Array.from(items).indexOf(current) : -1;

        if (event.key === 'ArrowDown') {
            index = index < items.length - 1 ? index + 1 : 0;
        } else {
            index = index > 0 ? index - 1 : items.length - 1;
        }

        items.forEach(item => item.classList.remove('selected'));
        items[index].classList.add('selected');
        items[index].scrollIntoView({ block: 'nearest' });
        return;
    }

    // Handle Enter key
    if (event.key === 'Enter') {
        const selected = dropdown.querySelector('.search-result-item.selected');
        if (selected && selected.dataset.name) {
            selectSearchResult(selected.dataset.name);
        }
        return;
    }

    // Handle Escape key
    if (event.key === 'Escape') {
        dropdown.classList.remove('show');
        dropdown.innerHTML = '';
        return;
    }

    const query = event.target.value.trim();

    if (!query || query.length < 1 || !state.currentConfig) {
        dropdown.classList.remove('show');
        dropdown.innerHTML = '';
        return;
    }

    const results = [];

    // Search pipelines
    state.currentConfig.pipelines?.forEach(p => {
        const nameMatch = fuzzyMatch(p.name, query);
        const descMatch = p.description ? fuzzyMatch(p.description, query) : { match: false, score: 0 };
        if (nameMatch.match || descMatch.match) {
            results.push({
                name: p.name,
                type: 'pipeline',
                score: Math.max(nameMatch.score * 1.5, descMatch.score) // Boost name matches
            });
        }
    });

    // Search datasources
    state.currentConfig.datasources?.forEach(ds => {
        const nameMatch = fuzzyMatch(ds.name, query);
        const descMatch = ds.description ? fuzzyMatch(ds.description, query) : { match: false, score: 0 };
        if (nameMatch.match || descMatch.match) {
            results.push({
                name: ds.name,
                type: 'datasource',
                score: Math.max(nameMatch.score * 1.5, descMatch.score)
            });
        }
    });

    // Search auto-created datasources
    const autoSources = new Set();
    state.currentConfig.pipelines?.forEach(p => {
        p.input_sources?.forEach(s => autoSources.add(s));
        p.output_sources?.forEach(s => autoSources.add(s));
    });
    autoSources.forEach(name => {
        if (!state.currentConfig.datasources?.find(ds => ds.name === name)) {
            const nameMatch = fuzzyMatch(name, query);
            if (nameMatch.match) {
                results.push({
                    name: name,
                    type: 'datasource',
                    score: nameMatch.score * 1.5
                });
            }
        }
    });

    // Sort by score
    results.sort((a, b) => b.score - a.score);

    // Limit results
    const topResults = results.slice(0, 8);

    if (topResults.length === 0) {
        dropdown.innerHTML = '<div class="search-result-item text-muted">No matches found</div>';
        dropdown.classList.add('show');
        return;
    }

    dropdown.innerHTML = topResults.map((r, i) => `
        <div class="search-result-item${i === 0 ? ' selected' : ''}" data-name="${r.name}" onclick="selectSearchResult('${r.name}')">
            <span class="result-type ${r.type}">${r.type === 'pipeline' ? 'Pipeline' : 'Source'}</span>
            <span class="result-name">${highlightMatch(r.name, query)}</span>
        </div>
    `).join('');
    dropdown.classList.add('show');
}

export function selectSearchResult(nodeName) {
    // Hide dropdown and clear search
    const dropdown = document.getElementById('graph-search-results');
    const searchInput = document.getElementById('graph-search');
    dropdown.classList.remove('show');
    searchInput.value = '';

    // Find and click the node in the graph
    let found = false;
    d3.select("#graph").selectAll(".node").each(function() {
        const node = d3.select(this);
        const nodeTitle = node.select("title").text();
        if (nodeTitle === nodeName) {
            found = true;
            selectNode(nodeName, this);
        }
    });

    if (!found) {
        // Node might not be visible, just show details
        showNodeDetails(nodeName);
    }
}

// Close dropdown when clicking outside
document.addEventListener('click', function(e) {
    const dropdown = document.getElementById('graph-search-results');
    const searchInput = document.getElementById('graph-search');
    if (dropdown && !dropdown.contains(e.target) && e.target !== searchInput) {
        dropdown.classList.remove('show');
    }
});

export function fitGraph() {
    if (state.graphviz) state.graphviz.fit(true);
}

export function resetGraph() {
    if (state.graphviz) state.graphviz.resetZoom();
}

export function collapseAllGroups() {
    state.expandedGroups.clear();
    updateGraph();
}

export function toggleGroup(groupName) {
    if (state.expandedGroups.has(groupName)) {
        state.expandedGroups.delete(groupName);
    } else {
        state.expandedGroups.add(groupName);
    }
    updateGraph();
    // Refresh the details panel to update the button state
    showNodeDetails(groupName);
}

