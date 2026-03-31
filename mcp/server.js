#!/usr/bin/env node

import { readFileSync } from 'fs'
import { createInterface } from 'readline'
import {
    nodeInfo, upstreamOf, downstreamOf, blastRadius, blastRadiusDiagram,
    fileBlastRadius, fileBlastRadiusDiagram, backfillOrder, clusterInfo,
    ownersOf, nodesByPerson, searchNodes, graphSummary
} from './pipeviz.js'

// --- Config loading ---

let config = null
const configPath = process.argv[2] || process.env.PIPEVIZ_CONFIG

if (configPath) {
    try {
        config = JSON.parse(readFileSync(configPath, 'utf8'))
    } catch (e) {
        process.stderr.write(`pipeviz-mcp: failed to load ${configPath}: ${e.message}\n`)
    }
}

// --- MCP protocol ---

const TOOLS = [
    {
        name: 'node_info',
        description: 'Get full details about a pipeline or datasource node — its type, sources, outputs, schedule, links, and metadata.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The node name (pipeline or datasource)' } },
            required: ['name']
        }
    },
    {
        name: 'upstream_of',
        description: 'Get everything upstream of a node — all pipelines and datasources that feed into it, recursively.',
        inputSchema: {
            type: 'object',
            properties: {
                name: { type: 'string', description: 'The node to trace upstream from' },
                depth: { type: 'number', description: 'Max traversal depth (default: 50)' }
            },
            required: ['name']
        }
    },
    {
        name: 'downstream_of',
        description: 'Get everything downstream of a node — all pipelines and datasources that consume its output, recursively.',
        inputSchema: {
            type: 'object',
            properties: {
                name: { type: 'string', description: 'The node to trace downstream from' },
                depth: { type: 'number', description: 'Max traversal depth (default: 50)' }
            },
            required: ['name']
        }
    },
    {
        name: 'blast_radius',
        description: 'Compute the blast radius of a node — how many pipelines and sources are affected if this node breaks or is stale.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The node to compute blast radius for' } },
            required: ['name']
        }
    },
    {
        name: 'blast_radius_diagram',
        description: 'Get a Mermaid flowchart diagram of the blast radius of a node — visualize the full downstream impact graph.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The node to visualize blast radius for' } },
            required: ['name']
        }
    },
    {
        name: 'file_blast_radius',
        description: 'Given a source file path, find which pipelines it belongs to (via links.github) and compute their combined blast radius. Use this for PR review or CI to answer "what breaks if I touch this file?"',
        inputSchema: {
            type: 'object',
            properties: { file: { type: 'string', description: 'File path — absolute, relative, or just the repo-relative path (e.g. src/main/scala/.../Foo.scala)' } },
            required: ['file']
        }
    },
    {
        name: 'file_blast_radius_diagram',
        description: 'Given a source file path, find which pipelines it belongs to and return a Mermaid flowchart of the full blast radius. Perfect for PR reviews — paste the diagram to visualize impact.',
        inputSchema: {
            type: 'object',
            properties: { file: { type: 'string', description: 'File path — absolute, relative, or repo-relative' } },
            required: ['file']
        }
    },
    {
        name: 'backfill_order',
        description: 'Get the topologically sorted order to backfill all pipelines downstream of a node. Use this to plan staged backfills.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The starting node to backfill from' } },
            required: ['name']
        }
    },
    {
        name: 'cluster_info',
        description: 'Get all pipelines and datasources in a cluster.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The cluster name' } },
            required: ['name']
        }
    },
    {
        name: 'owners_of',
        description: 'Get the owners and users of a pipeline or datasource.',
        inputSchema: {
            type: 'object',
            properties: { name: { type: 'string', description: 'The node name' } },
            required: ['name']
        }
    },
    {
        name: 'nodes_by_person',
        description: 'Find all pipelines and datasources owned or used by a person. Fuzzy matches on name.',
        inputSchema: {
            type: 'object',
            properties: { person: { type: 'string', description: 'Person name or partial match (e.g. "alice", "data-eng")' } },
            required: ['person']
        }
    },
    {
        name: 'search_nodes',
        description: 'Fuzzy search for pipelines and datasources by name.',
        inputSchema: {
            type: 'object',
            properties: { query: { type: 'string', description: 'Search query' } },
            required: ['query']
        }
    },
    {
        name: 'graph_summary',
        description: 'Get a high-level summary of the entire pipeline graph — counts, groups, clusters, and all node names.',
        inputSchema: { type: 'object', properties: {} }
    }
]

function handleToolCall(name, args) {
    switch (name) {
        case 'node_info':       return nodeInfo(config, args.name)
        case 'upstream_of':     return upstreamOf(config, args.name, args.depth)
        case 'downstream_of':   return downstreamOf(config, args.name, args.depth)
        case 'blast_radius':          return blastRadius(config, args.name)
        case 'blast_radius_diagram':  return blastRadiusDiagram(config, args.name)
        case 'file_blast_radius':             return fileBlastRadius(config, args.file)
        case 'file_blast_radius_diagram':     return fileBlastRadiusDiagram(config, args.file)
        case 'backfill_order':    return backfillOrder(config, args.name)
        case 'cluster_info':    return clusterInfo(config, args.name)
        case 'owners_of':      return ownersOf(config, args.name)
        case 'nodes_by_person': return nodesByPerson(config, args.person)
        case 'search_nodes':    return searchNodes(config, args.query)
        case 'graph_summary':   return graphSummary(config)
        default:                return { error: `Unknown tool: ${name}` }
    }
}

function handleMessage(msg) {
    const { id, method, params } = msg

    if (method === 'initialize') {
        return {
            jsonrpc: '2.0', id,
            result: {
                protocolVersion: '2024-11-05',
                capabilities: { tools: {} },
                serverInfo: { name: 'pipeviz-mcp', version: '1.0.0' }
            }
        }
    }

    if (method === 'notifications/initialized') return null

    if (method === 'tools/list') {
        return { jsonrpc: '2.0', id, result: { tools: TOOLS } }
    }

    if (method === 'tools/call') {
        const result = handleToolCall(params.name, params.arguments || {})
        return {
            jsonrpc: '2.0', id,
            result: { content: [{ type: 'text', text: JSON.stringify(result, null, 2) }] }
        }
    }

    return { jsonrpc: '2.0', id, error: { code: -32601, message: `Method not found: ${method}` } }
}

// --- stdio transport ---

const rl = createInterface({ input: process.stdin })
let buffer = ''

process.stdin.on('data', (chunk) => {
    buffer += chunk.toString()
    let newlineIdx
    while ((newlineIdx = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, newlineIdx).trim()
        buffer = buffer.slice(newlineIdx + 1)
        if (!line) continue
        try {
            const msg = JSON.parse(line)
            const response = handleMessage(msg)
            if (response) process.stdout.write(JSON.stringify(response) + '\n')
        } catch (e) {
            process.stderr.write(`pipeviz-mcp: parse error: ${e.message}\n`)
        }
    }
})

process.stderr.write(`pipeviz-mcp: ready${configPath ? ` (config: ${configPath})` : ' (no config — set PIPEVIZ_CONFIG)'}\n`)
