# data-ai-fde

A transit operations assistant built end to end: a data platform that captures and
models TransLink GTFS feeds, an agent service that answers from it, and a website
that hosts the chat and a blog.

| Directory | What | Stack |
| --- | --- | --- |
| [`data-platform/`](data-platform/) | Feed capture, raw snapshot preservation, Databricks Bronze → dbt Silver/Gold, Genie space | Python, GCP Cloud Run Jobs, Terraform, Databricks, dbt |
| [`agent-service/`](agent-service/) | Agent API (not started) | FastAPI, LangGraph, Pydantic AI |
| [`web/`](web/) | Map page, chat panel, blog | Astro, Starlight |

Each directory's README covers setup, checks and deployment. Interfaces between
them are explicit: the agent service reads the data platform through Unity Catalog
objects such as the `route_delay_summary` function, never through its tables'
internals.
