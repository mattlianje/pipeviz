# <img src="pix/stageview.png" width="50"> Pipeviz
**Visualize any data platform, with just a .json**

**Pipeviz** makes your OLAP codebase navigable, without touching how it runs.  Use a dead-simple JSON spec to define your pipelines and tables - from that alone, it builds:
- your lineage graph (DOT / Graphviz)
- a microfrontend/UI

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
