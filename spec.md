# Pipeviz JSON Specification (v1.1)

A single JSON file that declares your data system. No agents, no framework buy-in

**_Programmer oath_** (In semi-serious good-nature)
> I will declare my data system truthfully and completely in my `.json`
>
> I will not omit a pipeline or data source I know exists.
>
> I will generate this file automatically from my source of truth
so that it reflects reality without drift or manual edits.

## Goals / Non-Goals
- **Goals:** portable lineage, zero runtime coupling, vendor-agnostic, mergeable across teams.
- **Non-Goals:** execution/orchestration, column-level lineage, policy/ACLs, runtime telemetry.

## File
- Filetype: `.json`
- Encoding: UTF-8
- Top-level fields:
  - `pipelines` (required) — array of Pipeline
  - `datasources` (optional) — array of DataSource
  - `clusters` (optional) — array of Cluster
  - `version` (optional) — string, spec version (e.g. "1.0")

## Identifiers
- `name` values are case-sensitive, unique within their type
- Allowed chars: letters, digits, `_ - . / :`
- A node may belong to at most one `cluster`

## Pipeline
```json
{
  "name": "analytics-aggregation",  // Required
  "description": "Daily metrics",
  "input_sources": ["enriched_users", "processed_orders"],
  "output_sources": ["daily_metrics"],
  "upstream_pipelines": ["user-enrichment"],
  "schedule": "Daily 01:00",
  "tags": ["analytics","daily"],
  "cluster": "analytics",
  "links": { "airflow": "https://...", "monitoring": "https://..." }
}
```

**Rules**
- `name` required.
- `input_sources` / `output_sources` declare data flow to/from DataSource nodes.
- `upstream_pipelines` draws pipeline-to-pipeline dependencies.

All fields are additive, unknown fields are ignored by renderers.

## DataSource
```json
{
  "name": "enriched_users",   // Required
  "description": "Enriched user profile table",
  "type": "snowflake",
  "owner": "data@company.com",
  "tags": ["pii","users"],
  "cluster": "user-processing",
  "metadata": { "schema": "ANALYTICS", "size": "2.1TB" },
  "links": { "docs": "https://..." }
}
```
**Rules**
- `name` required
- If a DataSource is referenced by a Pipeline but not defined here, it is auto-created with type: "auto-created"

## Cluster
```json
{
  "name": "real-time",   // Required
  "description": "Streaming workloads",
  "parent": "order-management"
}
```
**Rules**
- Clusters are optional - referenced names that lack a definition are implicitly created
- Nesting via `parent` is supported

## Rendering Semantics
- Nodes: Pipeline -> box, DataSource -> ellipse, Cluster -> subgraph.
- Edges:
   - `source -> pipeline` and `pipeline -> source` from `input_sources` / `output_sources`.
   - `upstream -> pipeline` (distinct style) for logical dependencies.
- Layout is implementation-defined; semantics come from edges, not coordinates.

## Validation
- Minimal valid file requires `pipelines: [{ "name": "…" }]`.
- Recommended checks:
    - uniqueness of `name` per type,
    - referenced `upstream_pipelines` exist,
    - referenced `clusters` resolve (or are implicitly created).

## Compatibility & Versioning
- The spec is forward-additive: new optional fields may appear without breaking consumers.
- Include "version" when you need to pin behavior - renderers **_should_** treat unknown versions as the latest compatible.

## Generation Guidance
- Best practice: generate a `.json` at build time from your source of truth.
- Avoid hand-editing - manual edits risk drift and stale lineage.
- Each team can emit a file - merge by concatenating arrays then de-dupe on name.

## Semi-realistic example
```json
{
  "pipelines": [
    { "name": "user-enrichment",
      "input_sources": ["raw_users"],
      "output_sources": ["enriched_users"],
      "schedule": "Hourly" }
  ],
  "datasources": [
    { "name": "raw_users", "type": "snowflake" }
  ]
}
```

## Changelog
> All changes are backwards-compatible unless marked ⚠️ breaking.

- **1.1** - Added version top-level field.
   - Renderer should treat unknown versions as latest compatible.
- **1.0**- Initial release.
   - Required pipelines array with name.
   - Optional datasources and clusters.
   - Support for input_sources, output_sources, upstream_pipelines, tags, cluster, links, metadata.
   - Auto-creation of datasources/clusters when referenced.
