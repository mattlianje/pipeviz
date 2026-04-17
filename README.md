<div align="right">
  <sub><em>Part of <a href="https://github.com/mattlianje/d4"><img src="https://raw.githubusercontent.com/mattlianje/d4/master/pix/d4.png" width="23"></a> <a href="https://github.com/mattlianje/d4">d4</a></em></sub>
</div>

<p align="center">
  <img src="pix/pipeviz-2.png" width="700">
</p>

# <img src="https://raw.githubusercontent.com/mattlianje/d4/refs/heads/master/pix/pipeviz.png" width="60"> Pipeviz
**Easy, elegant lineage from a single `.json` 🛰️**

A JSON spec for lineage. Declare your pipelines, get a graph.

- No backend required, static hosting
- Works with any stack: SQL, Spark, Kafka, S3, APIs, shell scripts
- Each team owns their JSON, merge with `jq` for the org-wide view
- Column-level lineage built in
- Blast radius to see what breaks
- Backfill stages to parallelize safely

## Why?
When a data org doesn't have a trusted dependency graph:
- **Impact analysis is hard.** You can't trace where bad data came from or what downstream jobs will break.
- **Onboarding is slow.** New hires have no way to see the big picture.
- **Backfills are painful.** No clear order of operations when things need to be re-run.

Current tools have gaps:
- **Runtime instrumentation has a heavy upfront cost** ([OpenLineage](https://github.com/open-metadata/OpenMetadata), [Marquez](https://github.com/MarquezProject/marquez)) needs agents, metadata stores, scheduler integration.
- **Frameworks are brittle** ([dbt](https://github.com/dbt-labs/dbt-core)) couple you to their dialect, manifest, and world view.
- **Manual docs rot** (immediately).

With Pipeviz, you declare your immediate dependencies (preferably) at compile time: "this code reads from A, writes to B". Pipeviz stitches these declarations into complete end-to-end lineage.

## Of note...
Essentially, pipeviz is just a bunch of rendering niceties on top of a plain JSON spec... but this
means you can throw the raw JSON into any LLM and get results.

## Quickstart
[Live demo](https://pipeviz.org)

This pipeviz:
```json
{
  "pipelines": [
    {
      "name": "user-enrichment",
      "input_sources": ["raw_users"],
      "output_sources": ["enriched_users"]
    }
  ]
}
```
Renders this:

<p align="center">
  <img src="pix/pipeviz-example-2.png" width="600">
</p>

Attribute-level lineage (from the full demo):

<p align="center">
  <img src="pix/pipeviz-attributes.png" width="600">
</p>

## Usage
Go to [pipeviz.org](https://pipeviz.org) and paste your JSON, or drag-and-drop a file.

To auto-load a config, use `?url=`:
```
https://pipeviz.org?url=https://yoursite.com/pipeviz.json
```

## Attribute Lineage
Track column-level provenance with `::` notation. Supports infinitely nested complex data-types.

- `user_id` derives from a single upstream column
- `full_name` derives from multiple columns (array syntax)
- `city` accesses a nested field via `::` (`address.city` becomes `address::city`)

<p align="center">
  <img src="pix/pipeviz-attributes-example-2.png" width="600">
</p>

<details>
<summary>Full example JSON</summary>

```json
{
   "pipelines": [
      {
         "name": "user-etl",
         "input_sources": [
            "raw_users"
         ],
         "output_sources": [
            "enriched_users"
         ]
      }
   ],
   "datasources": [
      {
         "name": "raw_users",
         "attributes": [
            {
               "name": "id"
            },
            {
               "name": "first"
            },
            {
               "name": "last"
            },
            {
               "name": "address",
               "attributes": [
                  {
                     "name": "city"
                  },
                  {
                     "name": "zip"
                  }
               ]
            }
         ]
      },
      {
         "name": "enriched_users",
         "attributes": [
            {
               "name": "user_id",
               "from": "raw_users::id"
            },
            {
               "name": "full_name",
               "from": [
                  "raw_users::first",
                  "raw_users::last"
               ]
            },
            {
               "name": "city",
               "from": "raw_users::address::city"
            }
         ]
      }
   ]
}
```
</details>

## Merging Team Configs

Each team maintains their own `pipeviz.json`. Merge them with `jq`:

```bash
# simple merge
jq -s '{
  pipelines: map(.pipelines // []) | add,
  datasources: map(.datasources // []) | add
}' team-*.json > pipeviz.json
```

If teams have pipelines or datasources with the same `name`, last one wins. To keep both, prefix with team name:

```bash
# prefix names with filename to avoid collisions
for f in team-*.json; do
  team=$(basename "$f" .json)
  jq --arg t "$team" '
    .pipelines[]?.name |= "\($t)/\(.)" |
    .datasources[]?.name |= "\($t)/\(.)"
  ' "$f"
done | jq -s '{
  pipelines: map(.pipelines // []) | add,
  datasources: map(.datasources // []) | add
}' > pipeviz.json
```

Or dedupe by name, merging properties from all sources:

```bash
# merge properties for duplicate names
jq -s '{
  pipelines: (map(.pipelines // []) | add | group_by(.name) | map(add)),
  datasources: (map(.datasources // []) | add | group_by(.name) | map(add))
}' team-*.json > pipeviz.json
```

## What you get from the graph

- Topological sort into parallel stages for backfills
- Cycle detection
- Diamond dependency handling
- Blast radius analysis (downstream impact)
- Mermaid export
- MCP-ready JSON graph for LLM tooling

## MCP Server

A zero-dependency [MCP](https://modelcontextprotocol.io) server in `/mcp`. Point it at your pipeviz JSON and your LLM gets the full graph as tools.

### Install (one-liner)

```bash
curl -s https://pipeviz.org/mcp-install | sh -s /path/to/pipeviz.json
```

This adds the `pipeviz` MCP server to your `~/.claude.json` via `npx`. Restart Claude Code to pick it up.

### Manual setup

Add to your Claude Code config (`~/.claude.json`):
```json
"mcpServers": {
  "pipeviz": {
    "command": "npx",
    "args": ["pipeviz-mcp", "/path/to/pipeviz.json"]
  }
}
```

Or set `PIPEVIZ_CONFIG` env var and omit the second arg.

```
node_info              Details of a pipeline or datasource
upstream_of            Everything that feeds into a node
downstream_of          Everything that consumes a node's output
blast_radius           What breaks if this node goes down (+ owners to notify)
blast_radius_diagram   Mermaid flowchart of the blast radius
file_blast_radius      Blast radius from a source file path (via links.github)
backfill_order         Topologically sorted stages to re-run downstream
cluster_info           All pipelines and datasources in a cluster
owners_of              Who owns and uses a node
nodes_by_person        What does a person own or use
search_nodes           Fuzzy search by name
graph_summary          High-level counts, groups, clusters
```

Run tests with `make test` from `js/`.

## API

There's an optional Clojure web-server if you want programmatic access to the graph...

This is especially useful for cases where you want to let other people routinely "get answers" from your graph ("what would break if?", "what is
downstream of that?") without poking digging into a UI.


<p align="center">
  <img src="pix/swagger-demo.png" width="600">
</p>

## Why Clojure?

- Pipeviz, like Clojure has the code-as-data philosophy.
- No required persistence layer. Since Clojure is [homoiconic](https://en.wikipedia.org/wiki/Homoiconicity) you can REPL into, poke at, update, define listeners and hooks on your graph 
without having to restart your server or drop-down into the relational algebra of CRUD apps and ORM's. Your graph becomes very easy to extend, almost like a living organism.

## Inspiration
- The LISP [code-as-data](https://en.wikipedia.org/wiki/Code_as_data) ethos
- [Data-Oriented Programming](https://www.manning.com/books/data-oriented-programming) by Yehonathan Sharvit
