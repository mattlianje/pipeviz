<p align="center">
  <img src="pix/pipeviz.png" width="700">
</p>

# 🛰️ Pipeviz
**Easy, elegant lineage with a single `.json`**

Pipeviz is a dead simple JSON spec to describe your pipelines and tables. Drop in a `pipeviz.json`, and get:

- a dependency graph (DOT / Graphviz)
- a full microfrontend UI
- a clear map of your data system

You define what exists. **Pipeviz** draws the lines ✏️✨



## Features
- Declarative JSON format
- Graphviz diagrams, dashboards for free
- No coupling to Airflow, dbt, Spark, or vendor tooling
- Works across SQL, Delta, Kafka, S3, APIs (**_any_** stack or language)
- Each team can emit their own `pipeviz.json` - just merge
- Zero runtime hooks, agents or daemons

## Example
```json
{
  "pipelines": [
    {
      "name": "user-enrichment",
      "input_tables": ["raw_users"],
      "output_tables": ["enriched_users"],
      "tags": ["user", "ml"],
      "schedule": "Hourly",
      "links": {
        "af": "https://airflow.company.com/user_enrichment",
        "monitoring": "https://grafana.company.com/user_enrichment"
      }
    }
  ],
  "datasources": [
    {
      "name": "raw_users",
      "type": "snowflake",
      "metadata": {
        "schema": "RAW_DATA",
        "size": "2.1TB"
      },
      "tags": ["pii"]
    }
  ]
}
```

## Motivation
Lineage in most modern data stacks is an afterthought: bolted on through log scraping, runtime hooks, or vendor dashboards.

[OpenLineage](https://openlineage.io/), [Marquez](https://marquezproject.ai/), and [Atlas](https://atlas.apache.org/#/) generally assume you’ll instrument the runtime behaviour of your OS processes, buffer everything into the sockets of a central orchestrator, and accept whatever graph their agents extract.

That might work in theory - but not (easily) in large, polyglot OLAP codebases where:
- DAGs live in Scala, SQL, Python, shell scripts
- Data moves between different databases, warehouses, messages brokers, RPC services and API's
- Teams own pipelines independently, with no shared runtime

Pipeviz is a dead simple reorientation. It says: **_"You already know your pipelines and tables. Just declare them"_**

Each team owns a `pipeviz.json` that they generate how best they see fit (preferably at compile time) ... you merge them, you get the map.
