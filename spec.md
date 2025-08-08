# Pipeviz JSON Specification (v1.0)

## Schema
```json
{
  "pipelines": [              // Required
    {
      "name": "string",       // Required
      "description": "string",
      "input_sources": ["string"],
      "output_sources": ["string"],
      "schedule": "string",
      "tags": ["string"],
      "cluster": "string",
      "upstream_pipelines": ["string"],  // Dependencies
      "links": { "key": "url" }
    }
  ],
  "datasources": [            // Optional
    {
      "name": "string",       // Required
      "description": "string",
      "type": "string",
      "owner": "string",
      "tags": ["string"],
      "cluster": "string",
      "metadata": { "key": "value" },
      "links": { "key": "url" }
    }
  ],
  "clusters": [               // Optional
    {
      "name": "string",       // Required
      "description": "string",
      "parent": "string"      // Creates hierarchy
    }
  ]
}
```

## Behavior

- Referenced clusters and datasources are created implicitly
- Names must be unique within their type
- For rendering `input_sources` → `pipeline` → `output_sources` creates data flow, `upstream_pipelines` creates dependencies

## Real-world example
```json
{
  "clusters": [
    { "name": "ingestion" },
    { "name": "real-time", "parent": "ingestion" }
  ],
  "pipelines": [
    {
      "name": "events-pipeline",
      "input_sources": ["api"],
      "output_sources": ["events-table"],
      "cluster": "real-time",
      "upstream_pipelines": ["auth-pipeline"],
      "links": { "airflow": "https://..." }
    }
  ],
  "datasources": [
    {
      "name": "events-table",
      "type": "snowflake",
      "owner": "data-team@co.com",
      "metadata": { "size": "2TB" }
    }
  ]
}
```
