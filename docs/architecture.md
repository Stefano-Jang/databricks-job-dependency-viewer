# JIIG 2.0 Architecture

## System overview

JIIG is a read-only incident intelligence application backed by three scheduled SQL outputs. It uses native Databricks system tables for workload state, metadata, and observed table lineage.

```text
┌────────────────────────────────────────────────────────────┐
│ Databricks system tables                                   │
│ table_lineage · jobs · pipelines · run/update timelines    │
└──────────────────────────┬─────────────────────────────────┘
                           ▼
                  lineage_edges
                EDGE / SHARED_TABLE
                    ┌──────┴──────┐
                    ▼             ▼
           dag_relationships   jiig_dashboard
            graph snapshot      impact pairs
                    │             │
                    ▼             ▼
                JIIG App       AI/BI dashboard
```

The bundle job executes `jiig_lineage_edges` first. `jiig_dag_table` and `jiig_dashboard_table` then run in parallel from that canonical edge layer.

## Layer 1 — Canonical lineage edges

**Source:** `src/job/jiig-lineage-edges.sql`
**Output:** `lineage_edges`

### Edge kinds

| Kind | Direction | Evidence |
|---|---|---|
| `DEPENDENCY` | table producer → table reader | `system.access.table_lineage` with `direct_access = true` |
| `TRIGGER` | Job → Pipeline | `pipeline_update_timeline.trigger_details.job_task.job_id` |
| `TABLE_TRIGGER` | table producer → triggered Job | latest `system.lakeflow.jobs.trigger.table_update.table_names` |

Only Jobs and Pipelines produce tables. Dashboards, Genie spaces, queries, and alerts are terminal read-only consumers.

Parallel edge kinds between the same producer and consumer are merged into one row. `edge_kinds` and `edge_tables` preserve all observed evidence without multiplying graph paths.

### Shared-write protection

A table written by more than `max_table_fanout` distinct producers becomes a `SHARED_TABLE` record. It does not create dependency edges because a writers × readers join would create a false and potentially enormous cartesian graph. The app exposes these records as governance signals.

### Output contract

| Column | Meaning |
|---|---|
| `result_type` | `EDGE` or `SHARED_TABLE` |
| `producer_kind`, `producer_key` | Upstream entity identity |
| `consumer_kind`, `consumer_key` | Downstream entity identity |
| `edge_kinds` | Merged dependency/trigger evidence |
| `edge_tables` | Tables connecting producer and consumer |
| `edge_table_count` | Number of connecting tables |
| `producer_lineage_owner`, `consumer_lineage_owner` | `created_by` fallback from lineage |
| `table_full_name`, `writer_count` | Shared-table fields |

## Layer 2 — Graph snapshot

**Source:** `src/job/jiig-dag-table-creation.sql`
**Output:** `dag_relationships`

### Open failure state

Timeline rows are first aggregated to one record per Job run or Pipeline update. JIIG then finds the latest run/update per entity inside `failure_lookback_days`.

- Job open states: `FAILED`, `ERROR`, `TIMED_OUT`
- Pipeline open state: `FAILED` for `REFRESH` or `FULL_REFRESH`
- A newer successful run/update means the entity is not currently failed.
- `failure_count` remains the number of failed runs/updates in the lookback window.

### Node universe

The graph contains entities that are failed, recently active, or touch a canonical edge. Current Jobs/Pipelines use the latest SCD2 metadata row. When metadata is unavailable, an `UNREGISTERED` node is retained from run history or lineage and can still carry failure state and owner fallback.

### Canonical identity

Graph IDs use `KIND:raw-id`, for example:

```text
JOB:123
PIPELINE:123
DASHBOARD:01ef...
```

The raw resource identifier remains in `entity_id`. This prevents cross-kind collisions while preserving deep links to supported workspace resources.

### Graph metrics

| Metric | Definition |
|---|---|
| `in_degree` | Direct upstream entities |
| `out_degree` | Direct downstream entities |
| `downstream_reach` | Distinct downstream entities within the configured depth, clamped to five |
| `criticality_rank` | Downstream reach, then out-degree |
| `hub_rank` | Structural hub rank using the same reach/out-degree ordering |
| `authority_rank` | In-degree, then downstream reach |

Reach is implemented as five bounded join levels rather than an all-node recursive path expansion.

### Unified output contract

`dag_relationships` stores three shapes in one table:

- `result_type = 'NODES'`: identity, metadata, failure state, `snapshot_time`, degree/reach/rank metrics.
- `result_type = 'EDGES'`: canonical `source_id`, canonical `target_id`, connecting tables and edge kinds.
- `result_type = 'SHARED_TABLES'`: excluded shared-write tables and writer count.

`snapshot_time` records when the graph table was rebuilt. It is the app's freshness source; workload activity is not used as a proxy for snapshot freshness.

## Layer 3 — Impact pairs

**Source:** `src/job/jiig-dashboard-table-creation.sql`
**Output:** `jiig_dashboard`

The impact query starts from open failed Jobs/Pipelines and recursively follows canonical edges to `impact_max_depth`. Recursion tracks visited canonical keys to avoid cycles.

For each failed/affected pair it keeps:

- Shortest `hop_distance`.
- All intermediate tables observed on shortest paths.
- One deterministic human-readable `impact_path` that alternates entity and table/trigger evidence.
- Affected entity metadata and owner fallback.

A failed entity with no downstream consumer still produces one row with null affected fields so the managed dashboard retains the incident.

## Application query strategy

**Files:** `src/apps/app.py`, `src/apps/conn.py`, `src/apps/graph_utils.py`

The app does not load the workspace graph during normal incident triage:

1. `load_incidents` retrieves open failures inside the selected UI window.
2. `load_incident_signals` computes depth-specific reach counts and failed-to-failed multi-hop pairs server-side.
3. `load_subgraph` retrieves only the selected root, its bounded downstream graph, and direct upstream context.
4. Python BFS reconstructs shortest causal paths from the scoped edge set.
5. `load_leaders` retrieves server-ranked hub/authority lists.
6. `load_graph` is used only when the user opens Dependency Explorer.

Every cached query includes the current user key so forwarded-user results do not share a cache partition.

## Likely root/cascade classification

The app collapses any path between failed nodes, including healthy intermediates. A downstream failure becomes **likely cascade** when:

1. A failed upstream reaches it inside the selected depth, and
2. The upstream failure time is earlier than or equal to the downstream failure time.

The label deliberately says “likely.” Static lineage and aggregate failure timestamps do not prove causality.

## Runtime and deployment

- `run_app.py` binds Streamlit to `0.0.0.0` and `DATABRICKS_APP_PORT`.
- Streamlit CORS and XSRF protection are disabled because Databricks Apps supplies the reverse proxy boundary.
- The app prefers the forwarded user access token for SQL and falls back to the app service principal.
- Bundle targets contain no hard-coded workspace host, user root path, or user permission.
- `workspace_id` is a required bundle variable with no placeholder default.

## Scale and correctness boundaries

- UI and precomputed reach are limited to five hops.
- Incident exposure is based on observed lineage, not exact failed-task outputs.
- Lineage outside `lineage_lookback_days` is absent.
- Shared-write capping can exclude a legitimate wide-write dependency.
- Hub and authority ranks are structural, not weighted business centrality.
- Owners derived from `created_by` are fallback contacts, not guaranteed service ownership.
