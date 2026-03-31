import { strict as assert } from 'assert'
import {
    buildMaps, walk, topoSort, allNodeNames,
    nodeInfo, upstreamOf, downstreamOf, blastRadius, blastRadiusDiagram,
    fileBlastRadius, fileBlastRadiusDiagram, backfillOrder, clusterInfo,
    ownersOf, nodesByPerson, searchNodes, graphSummary
} from './pipeviz.js'

let passed = 0
let failed = 0

function test(name, fn) {
    try {
        fn()
        passed++
        process.stdout.write(`  \x1b[32m✓\x1b[0m ${name}\n`)
    } catch (e) {
        failed++
        process.stdout.write(`  \x1b[31m✗\x1b[0m ${name}\n    ${e.message}\n`)
    }
}

// --- Test fixtures ---

const CONFIG = {
    pipelines: [
        {
            name: 'ingest',
            input_sources: ['raw_events'],
            output_sources: ['clean_events'],
            cluster: 'ingestion',
            schedule: '*/15 * * * *',
            owners: ['alice', 'data-eng'],
            users: ['analytics-team'],
            links: { github: 'https://github.com/acme/repo/blob/master/src/main/scala/com/acme/ingest/IngestPipeline.scala' }
        },
        {
            name: 'enrich',
            input_sources: ['clean_events', 'user_dim'],
            output_sources: ['enriched_events'],
            upstream_pipelines: ['ingest'],
            cluster: 'processing',
            owners: ['bob'],
            users: ['alice', 'analytics-team'],
            links: { github: 'https://github.com/acme/repo/blob/master/src/main/scala/com/acme/enrich/EnrichPipeline.scala' }
        },
        {
            name: 'aggregate',
            input_sources: ['enriched_events'],
            output_sources: ['daily_metrics', 'hourly_metrics'],
            upstream_pipelines: ['enrich'],
            cluster: 'processing',
            owners: ['alice'],
            links: { github: 'https://github.com/acme/repo/blob/master/src/main/scala/com/acme/enrich/EnrichPipeline.scala' }
        },
        {
            name: 'export-a',
            input_sources: ['daily_metrics'],
            output_sources: ['warehouse_a'],
            upstream_pipelines: ['aggregate'],
            group: 'exports'
        },
        {
            name: 'export-b',
            input_sources: ['daily_metrics'],
            output_sources: ['warehouse_b'],
            upstream_pipelines: ['aggregate'],
            group: 'exports'
        }
    ],
    datasources: [
        { name: 'raw_events', type: 'kafka', cluster: 'ingestion' },
        { name: 'user_dim', type: 'snowflake', cluster: 'processing' }
    ],
    clusters: [
        { name: 'ingestion', description: 'Data ingestion layer' },
        { name: 'processing', description: 'Core processing' }
    ]
}

// --- buildMaps ---

process.stdout.write('\nbuildMaps\n')

test('creates upstream and downstream maps', () => {
    const { upstream, downstream } = buildMaps(CONFIG)
    assert.ok(upstream['enrich'].includes('clean_events'))
    assert.ok(upstream['enrich'].includes('user_dim'))
    assert.ok(upstream['enrich'].includes('ingest'))
    assert.ok(downstream['ingest'].includes('enrich'))
    assert.ok(downstream['ingest'].includes('clean_events'))
})

test('datasource edges are bidirectional', () => {
    const { upstream, downstream } = buildMaps(CONFIG)
    assert.ok(downstream['raw_events'].includes('ingest'))
    assert.ok(upstream['ingest'].includes('raw_events'))
})

test('upstream_pipelines creates edges', () => {
    const { upstream, downstream } = buildMaps(CONFIG)
    assert.ok(upstream['aggregate'].includes('enrich'))
    assert.ok(downstream['enrich'].includes('aggregate'))
})

// --- walk ---

process.stdout.write('\nwalk\n')

test('walks full downstream chain', () => {
    const { downstream } = buildMaps(CONFIG)
    const chain = walk('raw_events', downstream)
    const names = chain.map((n) => n.name)
    assert.ok(names.includes('ingest'))
    assert.ok(names.includes('enrich'))
    assert.ok(names.includes('aggregate'))
    assert.ok(names.includes('export-a'))
})

test('respects max depth', () => {
    const { downstream } = buildMaps(CONFIG)
    const shallow = walk('raw_events', downstream, 1)
    const names = shallow.map((n) => n.name)
    assert.ok(names.includes('ingest'))
    assert.ok(!names.includes('aggregate'))
})

test('assigns correct depths', () => {
    const { downstream } = buildMaps(CONFIG)
    const chain = walk('raw_events', downstream)
    const ingest = chain.find((n) => n.name === 'ingest')
    assert.equal(ingest.depth, 1)
})

test('handles node with no neighbors', () => {
    const chain = walk('warehouse_a', {})
    assert.equal(chain.length, 0)
})

test('handles cycles without infinite loop', () => {
    const cyclicMap = { a: ['b'], b: ['c'], c: ['a'] }
    const chain = walk('a', cyclicMap)
    assert.ok(chain.length <= 3)
})

// --- topoSort ---

process.stdout.write('\ntopoSort\n')

test('returns correct topological order', () => {
    const { downstream } = buildMaps(CONFIG)
    const sorted = topoSort(['ingest', 'enrich', 'aggregate'], downstream)
    assert.ok(sorted.indexOf('ingest') < sorted.indexOf('enrich'))
    assert.ok(sorted.indexOf('enrich') < sorted.indexOf('aggregate'))
})

test('handles single node', () => {
    const sorted = topoSort(['ingest'], {})
    assert.deepEqual(sorted, ['ingest'])
})

test('handles parallel nodes', () => {
    const { downstream } = buildMaps(CONFIG)
    const sorted = topoSort(['export-a', 'export-b'], downstream)
    assert.equal(sorted.length, 2)
})

// --- allNodeNames ---

process.stdout.write('\nallNodeNames\n')

test('includes pipelines, datasources, and auto-created sources', () => {
    const names = allNodeNames(CONFIG)
    assert.ok(names.has('ingest'))
    assert.ok(names.has('raw_events'))
    assert.ok(names.has('warehouse_a'))
    assert.ok(names.has('daily_metrics'))
})

test('returns empty set for null config', () => {
    const names = allNodeNames(null)
    assert.equal(names.size, 0)
})

// --- nodeInfo ---

process.stdout.write('\nnodeInfo\n')

test('returns pipeline details', () => {
    const info = nodeInfo(CONFIG, 'ingest')
    assert.equal(info.node_type, 'pipeline')
    assert.equal(info.name, 'ingest')
    assert.deepEqual(info.input_sources, ['raw_events'])
    assert.equal(info.schedule, '*/15 * * * *')
})

test('returns explicit datasource details', () => {
    const info = nodeInfo(CONFIG, 'raw_events')
    assert.equal(info.name, 'raw_events')
    assert.equal(info.cluster, 'ingestion')
})

test('returns auto-created datasource', () => {
    const info = nodeInfo(CONFIG, 'warehouse_a')
    assert.equal(info.node_type, 'datasource')
    assert.equal(info.auto_created, true)
})

test('returns error for unknown node', () => {
    const info = nodeInfo(CONFIG, 'does-not-exist')
    assert.ok(info.error)
})

test('returns error when no config', () => {
    const info = nodeInfo(null, 'ingest')
    assert.ok(info.error)
})

// --- upstreamOf ---

process.stdout.write('\nupstreamOf\n')

test('finds all upstream nodes', () => {
    const result = upstreamOf(CONFIG, 'aggregate')
    const pNames = result.upstream_pipelines.map((n) => n.name)
    const sNames = result.upstream_sources.map((n) => n.name)
    assert.ok(pNames.includes('enrich'))
    assert.ok(pNames.includes('ingest'))
    assert.ok(sNames.includes('raw_events'))
    assert.ok(sNames.includes('user_dim'))
})

test('returns empty for root source', () => {
    const result = upstreamOf(CONFIG, 'raw_events')
    assert.equal(result.total, 0)
})

test('respects depth limit', () => {
    const result = upstreamOf(CONFIG, 'aggregate', 1)
    const pNames = result.upstream_pipelines.map((n) => n.name)
    assert.ok(pNames.includes('enrich'))
    assert.ok(!pNames.includes('ingest'))
})

// --- downstreamOf ---

process.stdout.write('\ndownstreamOf\n')

test('finds all downstream nodes', () => {
    const result = downstreamOf(CONFIG, 'ingest')
    const pNames = result.downstream_pipelines.map((n) => n.name)
    assert.ok(pNames.includes('enrich'))
    assert.ok(pNames.includes('aggregate'))
    assert.ok(pNames.includes('export-a'))
})

test('returns empty for leaf node', () => {
    const result = downstreamOf(CONFIG, 'warehouse_a')
    assert.equal(result.total, 0)
})

// --- blastRadius ---

process.stdout.write('\nblastRadius\n')

test('computes correct blast radius', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    assert.ok(result.affected_pipelines.includes('ingest'))
    assert.ok(result.affected_pipelines.includes('aggregate'))
    assert.ok(result.affected_sources.includes('daily_metrics'))
    assert.ok(result.affected_sources.includes('warehouse_a'))
})

test('deduplicates results', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    const allNames = [...result.affected_pipelines, ...result.affected_sources]
    assert.equal(allNames.length, new Set(allNames).size)
})

test('total matches sum of pipelines and sources', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    assert.equal(result.total_affected, result.affected_pipelines.length + result.affected_sources.length)
})

test('generates summary string', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    assert.ok(result.summary.includes('pipeline(s)'))
    assert.ok(result.summary.includes('raw_events'))
})

test('leaf node has zero blast radius', () => {
    const result = blastRadius(CONFIG, 'warehouse_b')
    assert.equal(result.total_affected, 0)
})

// --- blastRadiusDiagram ---

process.stdout.write('\nblastRadiusDiagram\n')

test('returns valid mermaid with graph LR header', () => {
    const result = blastRadiusDiagram(CONFIG, 'raw_events')
    assert.ok(result.mermaid.startsWith('graph LR'))
})

test('includes origin node with origin class', () => {
    const result = blastRadiusDiagram(CONFIG, 'raw_events')
    assert.ok(result.mermaid.includes('raw_events'))
    assert.ok(result.mermaid.includes(':::origin'))
})

test('includes downstream pipelines and sources', () => {
    const result = blastRadiusDiagram(CONFIG, 'raw_events')
    assert.ok(result.mermaid.includes('ingest'))
    assert.ok(result.mermaid.includes('enrich'))
    assert.ok(result.mermaid.includes('daily_metrics'))
})

test('includes edges with arrow syntax', () => {
    const result = blastRadiusDiagram(CONFIG, 'raw_events')
    assert.ok(result.mermaid.includes(' --> '))
})

test('leaf node returns minimal diagram', () => {
    const result = blastRadiusDiagram(CONFIG, 'warehouse_b')
    assert.equal(result.total_affected, 0)
    assert.ok(result.mermaid.includes('no downstream'))
})

test('total_affected matches blast radius', () => {
    const br = blastRadius(CONFIG, 'ingest')
    const diagram = blastRadiusDiagram(CONFIG, 'ingest')
    assert.equal(diagram.total_affected, br.total_affected)
})

test('no duplicate edges', () => {
    const result = blastRadiusDiagram(CONFIG, 'raw_events')
    const edgeLines = result.mermaid.split('\n').filter((l) => l.includes(' --> '))
    assert.equal(edgeLines.length, new Set(edgeLines).size)
})

// --- fileBlastRadius ---

process.stdout.write('\nfileBlastRadius\n')

test('matches pipeline by full github URL path', () => {
    const result = fileBlastRadius(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.matched_pipelines.includes('ingest'))
})

test('matches pipeline by absolute local path suffix', () => {
    const result = fileBlastRadius(CONFIG, '/Users/me/repo/src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.matched_pipelines.includes('ingest'))
})

test('computes downstream blast radius from matched file', () => {
    const result = fileBlastRadius(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.downstream_pipelines.includes('enrich'))
    assert.ok(result.downstream_pipelines.includes('aggregate'))
    assert.ok(result.total_affected > 1)
})

test('file shared by multiple pipelines matches all', () => {
    const result = fileBlastRadius(CONFIG, 'src/main/scala/com/acme/enrich/EnrichPipeline.scala')
    assert.ok(result.matched_pipelines.includes('enrich'))
    assert.ok(result.matched_pipelines.includes('aggregate'))
})

test('unmatched file returns empty result', () => {
    const result = fileBlastRadius(CONFIG, 'src/main/scala/com/acme/Unknown.scala')
    assert.equal(result.matched_pipelines.length, 0)
    assert.equal(result.total_affected, 0)
})

test('generates summary string', () => {
    const result = fileBlastRadius(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.summary.includes('pipeline(s)'))
    assert.ok(result.summary.includes('IngestPipeline.scala'))
})

// --- fileBlastRadiusDiagram ---

process.stdout.write('\nfileBlastRadiusDiagram\n')

test('returns mermaid diagram for matched file', () => {
    const result = fileBlastRadiusDiagram(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.mermaid.startsWith('graph LR'))
    assert.ok(result.mermaid.includes('ingest'))
    assert.ok(result.mermaid.includes(':::origin'))
})

test('includes downstream nodes in diagram', () => {
    const result = fileBlastRadiusDiagram(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.mermaid.includes('enrich'))
    assert.ok(result.mermaid.includes('aggregate'))
})

test('file matching multiple pipelines marks all as origin', () => {
    const result = fileBlastRadiusDiagram(CONFIG, 'src/main/scala/com/acme/enrich/EnrichPipeline.scala')
    assert.ok(result.matched_pipelines.includes('enrich'))
    assert.ok(result.matched_pipelines.includes('aggregate'))
    // Both should be origin-styled
    const originCount = (result.mermaid.match(/:::origin/g) || []).length
    assert.equal(originCount, 2)
})

test('unmatched file returns empty diagram', () => {
    const result = fileBlastRadiusDiagram(CONFIG, 'src/Unknown.scala')
    assert.equal(result.matched_pipelines.length, 0)
    assert.equal(result.total_affected, 0)
})

test('includes edges in diagram', () => {
    const result = fileBlastRadiusDiagram(CONFIG, 'src/main/scala/com/acme/ingest/IngestPipeline.scala')
    assert.ok(result.mermaid.includes(' --> '))
})

// --- backfillOrder ---

process.stdout.write('\nbackfillOrder\n')

test('returns pipelines in topological order', () => {
    const result = backfillOrder(CONFIG, 'ingest')
    const order = result.backfill_order.map((s) => s.pipeline)
    assert.ok(order.indexOf('ingest') < order.indexOf('enrich'))
    assert.ok(order.indexOf('enrich') < order.indexOf('aggregate'))
    assert.ok(order.includes('export-a'))
    assert.ok(order.includes('export-b'))
})

test('includes stage numbers', () => {
    const result = backfillOrder(CONFIG, 'ingest')
    result.backfill_order.forEach((s, i) => {
        assert.equal(s.stage, i + 1)
    })
})

test('includes schedule when available', () => {
    const result = backfillOrder(CONFIG, 'ingest')
    const ingestStage = result.backfill_order.find((s) => s.pipeline === 'ingest')
    assert.equal(ingestStage.schedule, '*/15 * * * *')
    const enrichStage = result.backfill_order.find((s) => s.pipeline === 'enrich')
    assert.equal(enrichStage.schedule, null)
})

test('single pipeline backfill', () => {
    const result = backfillOrder(CONFIG, 'export-a')
    assert.equal(result.total_stages, 1)
    assert.equal(result.backfill_order[0].pipeline, 'export-a')
})

// --- clusterInfo ---

process.stdout.write('\nclusterInfo\n')

test('returns pipelines and datasources in cluster', () => {
    const result = clusterInfo(CONFIG, 'ingestion')
    assert.ok(result.pipelines.includes('ingest'))
    assert.ok(result.datasources.includes('raw_events'))
    assert.equal(result.description, 'Data ingestion layer')
})

test('returns error for unknown cluster', () => {
    const result = clusterInfo(CONFIG, 'nonexistent')
    assert.ok(result.error)
})

test('cluster with no datasources still works', () => {
    const config = { ...CONFIG, datasources: [] }
    const result = clusterInfo(config, 'ingestion')
    assert.ok(result.pipelines.includes('ingest'))
    assert.equal(result.datasources.length, 0)
})

// --- ownersOf ---

process.stdout.write('\nownersOf\n')

test('returns owners and users for pipeline', () => {
    const result = ownersOf(CONFIG, 'ingest')
    assert.deepEqual(result.owners, ['alice', 'data-eng'])
    assert.deepEqual(result.users, ['analytics-team'])
    assert.equal(result.node_type, 'pipeline')
})

test('returns empty arrays when no owners defined', () => {
    const result = ownersOf(CONFIG, 'export-a')
    assert.deepEqual(result.owners, [])
    assert.deepEqual(result.users, [])
})

test('returns empty for auto-created datasource', () => {
    const result = ownersOf(CONFIG, 'warehouse_a')
    assert.deepEqual(result.owners, [])
    assert.equal(result.auto_created, true)
})

test('returns error for unknown node', () => {
    const result = ownersOf(CONFIG, 'nope')
    assert.ok(result.error)
})

// --- nodesByPerson ---

process.stdout.write('\nnodesByPerson\n')

test('finds pipelines owned by person', () => {
    const result = nodesByPerson(CONFIG, 'alice')
    assert.ok(result.owns.some((n) => n.name === 'ingest'))
    assert.ok(result.owns.some((n) => n.name === 'aggregate'))
})

test('finds pipelines used by person', () => {
    const result = nodesByPerson(CONFIG, 'alice')
    assert.ok(result.uses.some((n) => n.name === 'enrich'))
})

test('case insensitive matching', () => {
    const result = nodesByPerson(CONFIG, 'ALICE')
    assert.ok(result.owns.length > 0)
})

test('partial match works', () => {
    const result = nodesByPerson(CONFIG, 'data-eng')
    assert.ok(result.owns.some((n) => n.name === 'ingest'))
})

test('returns empty for unknown person', () => {
    const result = nodesByPerson(CONFIG, 'nobody')
    assert.equal(result.total_owns, 0)
    assert.equal(result.total_uses, 0)
})

// --- blastRadius with owners ---

process.stdout.write('\nblastRadius owners\n')

test('blast radius includes affected owners', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    assert.ok(result.affected_owners.includes('alice'))
    assert.ok(result.affected_owners.includes('bob'))
})

test('blast radius summary mentions owners when present', () => {
    const result = blastRadius(CONFIG, 'raw_events')
    assert.ok(result.summary.includes('Owners to notify'))
})

test('blast radius has empty owners for ownerless leaf', () => {
    const result = blastRadius(CONFIG, 'warehouse_b')
    assert.deepEqual(result.affected_owners, [])
})

// --- searchNodes ---

process.stdout.write('\nsearchNodes\n')

test('finds pipelines by substring', () => {
    const result = searchNodes(CONFIG, 'export')
    assert.equal(result.count, 2)
    assert.ok(result.results.some((r) => r.name === 'export-a'))
    assert.ok(result.results.some((r) => r.name === 'export-b'))
})

test('finds datasources by substring', () => {
    const result = searchNodes(CONFIG, 'raw')
    assert.ok(result.results.some((r) => r.name === 'raw_events'))
})

test('case insensitive', () => {
    const result = searchNodes(CONFIG, 'INGEST')
    assert.ok(result.results.some((r) => r.name === 'ingest'))
})

test('no duplicates in results', () => {
    const result = searchNodes(CONFIG, 'events')
    const names = result.results.map((r) => r.name)
    assert.equal(names.length, new Set(names).size)
})

test('returns empty for no match', () => {
    const result = searchNodes(CONFIG, 'zzzzzzz')
    assert.equal(result.count, 0)
})

// --- graphSummary ---

process.stdout.write('\ngraphSummary\n')

test('returns correct counts', () => {
    const result = graphSummary(CONFIG)
    assert.equal(result.pipelines, 5)
    assert.equal(result.datasources, 2)
})

test('lists groups', () => {
    const result = graphSummary(CONFIG)
    assert.ok(result.groups.includes('exports'))
})

test('lists clusters', () => {
    const result = graphSummary(CONFIG)
    assert.ok(result.clusters.includes('ingestion'))
    assert.ok(result.clusters.includes('processing'))
})

test('lists all pipeline names', () => {
    const result = graphSummary(CONFIG)
    assert.ok(result.pipeline_names.includes('ingest'))
    assert.ok(result.pipeline_names.includes('export-a'))
})

test('returns error when no config', () => {
    const result = graphSummary(null)
    assert.ok(result.error)
})

// --- MCP protocol (integration) ---

process.stdout.write('\nMCP protocol\n')

import { spawn } from 'child_process'
import { writeFileSync, unlinkSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'

async function mcpCall(configPath, messages) {
    return new Promise((resolve, reject) => {
        const serverPath = new URL('./server.js', import.meta.url).pathname
        const proc = spawn('node', [serverPath, configPath])
        let stdout = ''
        let stderr = ''
        proc.stdout.on('data', (d) => (stdout += d.toString()))
        proc.stderr.on('data', (d) => (stderr += d.toString()))
        proc.stdin.write(messages.map((m) => JSON.stringify(m)).join('\n') + '\n')
        proc.stdin.end()
        proc.on('close', () => {
            const lines = stdout.trim().split('\n').filter(Boolean)
            resolve(lines.map((l) => JSON.parse(l)))
        })
        setTimeout(() => { proc.kill(); reject(new Error('timeout')) }, 5000)
    })
}

const tmpConfig = join(tmpdir(), `pipeviz-test-${Date.now()}.json`)
writeFileSync(tmpConfig, JSON.stringify(CONFIG))

try {
    const initMsg = { jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '0.1' } } }

    test('initialize returns server info', async () => {
        const [resp] = await mcpCall(tmpConfig, [initMsg])
        assert.equal(resp.result.serverInfo.name, 'pipeviz-mcp')
        assert.equal(resp.result.protocolVersion, '2024-11-05')
    })

    test('tools/list returns all 8 tools', async () => {
        const responses = await mcpCall(tmpConfig, [
            initMsg,
            { jsonrpc: '2.0', id: 2, method: 'tools/list', params: {} }
        ])
        const toolList = responses.find((r) => r.id === 2)
        assert.equal(toolList.result.tools.length, 13)
    })

    test('tools/call blast_radius returns content', async () => {
        const responses = await mcpCall(tmpConfig, [
            initMsg,
            { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'blast_radius', arguments: { name: 'raw_events' } } }
        ])
        const resp = responses.find((r) => r.id === 2)
        const body = JSON.parse(resp.result.content[0].text)
        assert.ok(body.affected_pipelines.length > 0)
    })

    test('tools/call node_info returns pipeline', async () => {
        const responses = await mcpCall(tmpConfig, [
            initMsg,
            { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'node_info', arguments: { name: 'ingest' } } }
        ])
        const resp = responses.find((r) => r.id === 2)
        const body = JSON.parse(resp.result.content[0].text)
        assert.equal(body.node_type, 'pipeline')
    })

    test('unknown method returns error', async () => {
        const responses = await mcpCall(tmpConfig, [
            initMsg,
            { jsonrpc: '2.0', id: 2, method: 'nonexistent/method', params: {} }
        ])
        const resp = responses.find((r) => r.id === 2)
        assert.ok(resp.error)
        assert.equal(resp.error.code, -32601)
    })

    // Wait for async tests
    await new Promise((r) => setTimeout(r, 3000))
} finally {
    try { unlinkSync(tmpConfig) } catch {}
}

// --- Summary ---

process.stdout.write(`\n${passed + failed} tests: \x1b[32m${passed} passed\x1b[0m`)
if (failed > 0) process.stdout.write(`, \x1b[31m${failed} failed\x1b[0m`)
process.stdout.write('\n\n')
process.exit(failed > 0 ? 1 : 0)
