# <img src="pix/stageview.png" width="50"> Pipeviz
**Elegant lineage, infinite scale**

**Pipeviz** makes your OLAP codebase navigable, without touching how it runs.  Use a dead-simple `pipeviz.json` spec to define your pipelines and tables, and get:
- Dependency graph (DOT / Graphviz)
- Microfrontend/UI

You just describe what exists. **Pipeviz** draws the lines.

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
