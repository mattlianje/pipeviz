# <img src="pix/stageview.png" width="50"> Pipeviz
**Easy, elegant lineage**

**Pipeviz** makes your OLAP codebase navigable, without touching how it runs.
Use a dead-simple `pipeviz.json` spec to describe your pipelines and tables - and get:

- a dependency graph (DOT / Graphviz)
- a full microfrontend UI
- a clear map of your data system

You define what exists. **Pipeviz** draws the lines.



## Features
- Define pipelines and tables in a simple, declarative JSON format
- Graphviz diagrams, dashboards for free
- No coupling to Airflow, dbt, Spark, or vendor tooling
- Works across SQL, Delta, Kafka, S3, APIs (**_any_** stack or language)
- Each team can emit their own `pipeviz.json` - just merge to compose the full graph
- Zero runtime hooks, no agents, no AST parsing

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

OpenLineage, Marquez, and Monte Carlo assume you’ll instrument every system, run everything through a central orchestrator, and accept whatever graph their agents extract.

That might work in theory - but not (easily) in large, polyglot OLAP codebases where:
- DAGs live in Scala, SQL, Python, shell scripts
- Data moves between Snowflake, Delta, Kafka, S3, and APIs
- Teams own pipelines independently, with no shared runtime

Pipeviz is simply a small reorientation. It says "You already know your pipelines and tables". Just declare them.
Each team owns a `pipeviz.json`, you merge them, you get the map.
