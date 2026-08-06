# JIIG: Job Incidents Identification Graph

When a Databricks Lakeflow Job or Pipeline fails, admins must work out which downstream Jobs, Pipelines, dashboards, and alerts consume the tables it produced, then notify those owners. This manual triage takes hours and is error-prone. **JIIG automates it**: a scheduled SQL job analyzes Databricks system tables, a dashboard surfaces failure-to-impact chains, and an incident-centric Streamlit app lets you explore the blast radius, find owners to notify, and draft notifications.

Everything is computed from **native system tables only** — no auxiliary notebooks, no user-ID mapping tables, and no event-log scanning.

## What you get

| Component | Provides |
|-----------|----------|
| **Scheduled job** (every 30 min) | Three tables: shared lineage edges, graph nodes + edges, multi-hop failure impact pairs |
| **AI/BI dashboard** | Failed entities, affected downstream, hop distance, impact path (`Job A → Pipeline B → Job C`), terminal consumers (dashboards/Genie/queries/alerts) |
| **Streamlit app** | Incident list (sorted by blast radius, classified as ROOT or CASCADE), incident subgraph, affected owners, critical tables, ready-to-send notification drafts |

### Screens

![Incidents](resources/figures/jiig_app_incidents.png)

![Graph](resources/figures/jiig_graph.png)

## Data sources and output tables

| System table | Feeds |
|---|---|
| `system.access.table_lineage` | Table dependency edges (direct_access only) |
| `system.lakeflow.jobs` / `pipelines` | Entity metadata, owner emails (SCD2, latest row) |
| `system.lakeflow.job_run_timeline` | Job failure detection |
| `system.lakeflow.pipeline_update_timeline` | Pipeline update failures, job→pipeline trigger links |

| Output table | Purpose | Rows |
|---|---|---|
| `lineage_edges` (shared) | Single source of truth for all edges: DEPENDENCY, TRIGGER, TABLE_TRIGGER | Direct + triggered |
| `dag_relationships` | Nodes (Jobs/Pipelines/Dashboards/etc.) + edges, graph metrics | All nodes + edges |
| `jiig_dashboard` | Multi-hop impact pairs: (failed entity, affected entity, hop_distance, path) | Failed × affected |

## Quick start

### 1. Install Databricks CLI

Requires v0.205 or later (the legacy `pip install databricks-cli` does not support bundles).

```bash
databricks --version

# macOS
brew tap databricks/tap && brew install databricks

# Linux/macOS (script)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

See: [Install the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install)

### 2. Authenticate and edit databricks.yml

```bash
databricks auth login --host https://your-workspace-url.cloud.databricks.com --profile your-profile-name
```

**REQUIRED EDITS before deploy:**

Open `databricks.yml` and set:

```yaml
variables:
  warehouse_id:
    lookup:
      warehouse: your-sql-warehouse-name  # Find with: databricks warehouses list
  workspace_id:
    default: "YOUR_NUMERIC_WORKSPACE_ID"  # From URL (?o=...) or Admin Settings
```

**Why this matters:** `workspace_id` filters all system-table queries. A wrong value silently returns zero rows instead of failing, so it looks like "no failures" rather than misconfiguration.

### 3. Deploy and run

```bash
databricks bundle deploy -t dev --profile your-profile-name

# Run the job immediately (otherwise waits 30 min for schedule)
databricks bundle run jiig_job -t dev --profile your-profile-name

# Start the app
databricks bundle run job_dependency_graph -t dev --profile your-profile-name
```

### 4. View results

- **Dashboard**: Find the deployed dashboard in your workspace under the specified catalog/schema.
- **App**: The Streamlit app starts in your browser. Select an incident to see its impact subgraph.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `warehouse_id` | (required) | SQL warehouse for all job tasks and app queries |
| `workspace_id` | `"0000000000000000"` | Numeric workspace ID (must be set correctly) |
| `jiig_catalog` | `shared` | Catalog where tables are created |
| `jiig_schema` | `jiig_${bundle.target}` | Schema (per-target isolation: dev vs. prod) |
| `account_name` | `MyCompany` | Dashboard title |
| `failure_lookback_days` | `7` | How many days back to scan for failures |
| `lineage_lookback_days` | `30` | How many days of table lineage define edges |
| `impact_max_depth` | `5` | Max downstream hops to follow |
| `max_table_fanout` | `20` | Tables written by >N distinct producers are excluded (see below) |
| `include_consumer_kinds` | `DASHBOARD,QUERY,GENIE,ALERT` | Leaf consumer types (read-only) to include |

### Key tuning: `max_table_fanout`

Hub tables — platform monitoring sinks like `system.data_quality_monitoring.table_results` (795 distinct writers) and user scratch tables (352 writers) — create a writers × readers cartesian product. On a large demo workspace, this alone inflated the edge set to **872,990 edges**. Setting `max_table_fanout = 20` cuts this to ~7,900 real edges (**-99%**) while excluding only 46 of ~30,600 tables. All 26,258 single-writer tables survive. Excluded tables are not hidden — they appear in the edge table as `SHARED_TABLE` records because "795 jobs write one table" is itself an insight.

## What's new in 2.0

### Edge explosion fixed
Hub tables with very many unrelated producers inflated edge counts. Version 2.0 caps writer fan-out at `max_table_fanout` and reports excluded tables separately.

### False edges removed
Pre-2.0 included transitive ancestors from `system.access.table_lineage` where `direct_access = false`. Only direct edges are now used.

### Failure counts corrected
Pre-2.0 used `COUNT(*)` over timeline rows; one run occupies multiple rows, inflating counts. Now uses `COUNT(DISTINCT run_id / update_id)`.

### Business impact is now visible
Pre-2.0 only tracked Job and Pipeline consumers. Version 2.0 also resolves **dashboards** (956 on test workspace), **Genie spaces** (929), **SQL queries** (760), and **alerts** (78) as terminal read-only consumers, so you see that a failure means a stale dashboard people are reading.

### Phantom job removed
`pipeline_update_timeline.trigger_details.job_task.job_id` sometimes holds sentinels like `SQL_SCHEDULE` (92k updates across 202 pipelines) instead of a job ID. Pre-2.0 would have invented a single fake job owning hundreds of pipelines, ranking it as the most critical entity in the workspace. Version 2.0 filters for numeric IDs only.

### Unregistered entities kept
905 of 1,491 job producers had no row in `system.lakeflow.jobs` (751 of them had run history; alert-backed jobs never appear there). Pre-2.0 dropped them, losing ~86% of edges. Version 2.0 keeps them as `UNREGISTERED` nodes.

### Single edge definition
Pre-2.0 derived edges in two separate SQL files that sometimes disagreed. Version 2.0 has one shared `lineage_edges` table that both the graph and dashboard tables read from.

### Graph layout
The bundled `dagre` hierarchical (top-down DAG) layout is now exposed in the app. Layout options: `dagre`, `breadthfirst`, `concentric`, `cose`, `circle`, `grid`, `fcose`, `cola`.

### Pipeline subtypes visible
Materialized Views and Streaming Tables (both pipeline kinds) now display as their table nature, not generic pipelines.

### Dashboard parameters automatic
Bundle variables `dataset_catalog` / `dataset_schema` inject catalog/schema into the dashboard at deploy time — no post-deploy manual editing required.

## Operations

### Schedule and manual refresh

Tables are rebuilt every 30 minutes by the scheduled job (`jiig_job`). To refresh immediately:

```bash
databricks bundle run jiig_job -t dev --profile your-profile-name
```

### Permissions

- **Workspace admin** OR
- Read access to `system.lakeflow.*` and `system.access.*` (system schemas must be enabled on metastore)

### App data freshness

The app queries the three tables directly; its data lags behind failures by at most the job schedule (30 min). The app caches graphs in memory (lossy LRU cache).

### On-behalf-of user auth

The app uses on-behalf-of-user (OBO) authorization to run queries as the logged-in user. This requires OBO to be enabled for Databricks Apps on your workspace.

![obo](resources/figures/jiig_obo.png)

## Limitations and known caveats

### System-table latency

Table lineage is indexed asynchronously by the Databricks platform. On a freshly created metastore, lineage can lag hours behind the first queries.

### Public Preview tables

`system.lakeflow.pipelines` and `system.lakeflow.pipeline_update_timeline` are in Public Preview and subject to change.

### `max_table_fanout` is a heuristic

A genuinely wide-fanout table (e.g., a platform-wide feature flag table written by 100 independent services) will be capped. To disable fanout capping, set `max_table_fanout` to a very large number.

### Owner display names can be numeric

Entities not modified since ~Dec 2025 have numeric user IDs instead of email addresses in `creator_user_name` / `run_as_user_name` (columns are not backfilled). The app falls back to the raw ID.

### Leaf consumers appear as ID prefixes

Dashboards, Genie spaces, queries, and alerts have no display name in system tables, so they appear as `Dashboard <id-prefix>`, `Genie <id-prefix>`, etc.

## Project structure

```
.
├── databricks.yml
├── README.md
├── docs
│   └── architecture.md          # Technical design notes
├── resources
│   ├── figures
│   │   ├── jiig_app_incidents.png
│   │   ├── jiig_graph.png
│   │   ├── jiig_obo.png
│   │   └── jiig_warehouse_id.png
│   ├── jiig.apps.yml            # Streamlit app resource
│   ├── jiig.dashboard.yml       # AI/BI dashboard resource
│   └── jiig.job.yml             # Job resource (3 SQL tasks)
├── scratch
│   └── README.md
└── src
    ├── apps
    │   ├── app.py               # Incident-centric Streamlit UI
    │   ├── app.yaml             # Fallback config for manual deploys
    │   ├── conn.py              # Warehouse connection (OBO, SP fallback)
    │   ├── graph_utils.py       # BFS, blast radius, insight helpers
    │   └── requirements.txt
    ├── dashboard
    │   └── jiig-dashboard.lvdash.json
    └── job
        ├── jiig-lineage-edges.sql                   # Shared edge layer
        ├── jiig-dag-table-creation.sql              # Graph nodes + edges
        └── jiig-dashboard-table-creation.sql        # Impact pairs
```

## Contribution

We welcome pull requests. Please share ideas on dashboard and graph improvements.

## Disclaimer

The main author (stefano-jang) is a Solutions Architect at Databricks. However, this project is not part of any official company work and was initiated solely as a personal good-will effort. It does not come with any guaranteed SLA, nor does it provide official support. If you intend to use this code in your production environment, please review it thoroughly and assume full responsibility for its use.
