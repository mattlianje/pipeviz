<p align="center">
  <img src="pix/pipeviz-2.png" width="700">
</p>

# <img src="https://raw.githubusercontent.com/mattlianje/d4/refs/heads/master/pix/pipeviz.png" width="60"> Pipeviz
**Easy, elegant lineage from a single `.json` 🛰️**

A JSON spec for lineage. Declare your pipelines, get a graph.

- One HTML file, no backend, no build step
- Works with any stack: SQL, Spark, Kafka, S3, APIs, shell scripts
- Each team owns their JSON, merge with `jq` for the org-wide view
- Column-level lineage built in

## Why
When a data org doesn't have a trusted dependency graph:
- **Impact analysis is hard.** What depends on this job? Where is bad data coming from?
- **Onboarding is slow.** New hires can't see the big picture.
- **Operational work is error-prone.** What to backfill? What to re-run? Backiflls get quite Byzantine.

Current tools have gaps:
- **Runtime instrumentation** (OpenLineage, Marquez) needs agents, metadata stores, scheduler integration.
- **Frameworks** (dbt) couple you to their dialect, manifest, and world view.
- **Manual docs** (Confluence diagrams) rot immediately.

With Pipeviz, declare your immediate dependencies at compile time: "this code reads from A, writes to B". Pipeviz stitches them into complete end-to-end lineage. Since it is plain JSON, you can throw it into any LLM and get results.

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

## Using
Go to [pipeviz.org](https://pipeviz.org) and paste your JSON, or drag-and-drop a file.

To auto-load a config, use `?url=`:
```
https://pipeviz.org?url=https://yoursite.com/pipeviz.json
```

To self-host, download [pipeviz.html](https://github.com/mattlianje/pipeviz/blob/master/pipeviz.html) and serve it from anywhere.

<details>
<summary>View a local JSON file (one-liner)</summary>

```bash
(curl -s https://raw.githubusercontent.com/mattlianje/pipeviz/master/pipeviz.html > /tmp/pipeviz.html && \
  cp /path/to/your/pipeviz.json /tmp/ && \
  echo "🛰️ Serving at http://localhost:8000" && \
  python3 -m http.server 8000 -d /tmp 2>/dev/null & PID=$!; \
  sleep 0.3; open "http://localhost:8000/pipeviz.html?url=http://localhost:8000/pipeviz.json"; \
  read -p "Press enter to stop..."; kill $PID 2>/dev/null; echo "✓ Stopped")
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

## Inspiration
- The LISP [code-as-data](https://en.wikipedia.org/wiki/Code_as_data) ethos
- [Data-Oriented Programming](https://www.manning.com/books/data-oriented-programming) by Yehonathan Sharvit
