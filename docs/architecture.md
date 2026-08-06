# JIIG Architecture

This document describes the SQL pipeline that builds JIIG's three output tables: `lineage_edges`, `dag_relationships`, and `jiig_dashboard`. It is intended for developers modifying the SQL logic.

## Overview

The pipeline is organized as a **three-layer DAG**:

```
1. jiig-lineage-edges.sql
   └─→ lineage_edges (EDGE + SHARED_TABLE rows)
         ├─→ jiig-dag-table-creation.sql
         │   └─→ dag_relationships (nodes + edges + graph metrics)
         └─→ jiig-dashboard-table-creation.sql
             └─→ jiig_dashboard (failed entity × affected entity pairs)
```

### Why the shared lineage layer?

Pre-2.0, the graph and dashboard queries each derived edges independently. They sometimes disagreed:

- Different handling of `direct_access` filtering
- Transitive ancestors treated as edges in one but not the other
- Inconsistent handling of hub tables

Having a single `lineage_edges` source of truth ensures both downstream queries see the same edges.

---

## Layer 1: Shared Lineage Edges

**File:** `src/job/jiig-lineage-edges.sql`

**Input:** Three native system tables
- `system.access.table_lineage` — table read/write lineage
- `system.lakeflow.pipeline_update_timeline` — job→pipeline orchestration triggers
- `system.lakeflow.jobs` — declared table-update triggers

**Output:** `lineage_edges` table

### Edge kinds

The table contains three types of edges, distinguished by `edge_kind`:

| Kind | Meaning | Source |
|---|---|---|
| `DEPENDENCY` | Producer writes a table, consumer reads it (direct table lineage) | `system.access.table_lineage` (filtered to `direct_access = true`) |
| `TRIGGER` | A job task started a pipeline update | `system.lakeflow.pipeline_update_timeline.trigger_details.job_task.job_id` |
| `TABLE_TRIGGER` | A job declares a table-update trigger (fires when specified tables change) | `system.lakeflow.jobs.trigger.table_update.table_names` |

All edges are merged on `(producer_kind, producer_key, consumer_kind, consumer_key)` so that parallel edge kinds don't multiply paths during traversal (one row per producer-consumer pair, even if they have DEPENDENCY + TRIGGER).

### Output schema

Each row in `lineage_edges` represents either an edge or a excluded hub table:

| Column | Type | Used for |
|---|---|---|
| `result_type` | STRING | `"EDGE"` or `"SHARED_TABLE"` |
| `producer_kind` | STRING | `"JOB"` or `"PIPELINE"` (only producers) |
| `producer_key` | STRING | Job ID or Pipeline ID |
| `consumer_kind` | STRING | `"JOB"`, `"PIPELINE"`, `"DASHBOARD"`, `"QUERY"`, `"GENIE"`, or `"ALERT"` |
| `consumer_key` | STRING | Entity ID |
| `edge_kinds` | ARRAY<STRING> | `["DEPENDENCY"]`, `["TRIGGER"]`, `["DEPENDENCY", "TRIGGER"]`, etc. |
| `edge_tables` | ARRAY<STRING> | Table names that form the dependency (for DEPENDENCY/TABLE_TRIGGER only; empty for TRIGGER) |
| `edge_table_count` | INT | Count of tables in `edge_tables` |
| `producer_lineage_owner` | STRING | Owner email from lineage (nullable; used for leaf consumers) |
| `consumer_lineage_owner` | STRING | Owner email from lineage (nullable; used for leaf consumers) |
| `table_full_name` | STRING | (Only for `result_type = "SHARED_TABLE"`) Table name |
| `writer_count` | INT | (Only for `result_type = "SHARED_TABLE"`) Number of distinct producers |

### Correctness rules

Two rules are enforced here and must not be broken:

#### Rule 1: Direct access only

```sql
WHERE l.direct_access
```

`system.access.table_lineage.direct_access = false` rows are transitive ancestors (A reads B, B reads C, so A indirectly reads C). Pre-2.0 treated them as direct edges, inventing dependencies that do not exist. Only `direct_access = true` rows are edges.

#### Rule 2: Cap writer fan-out

```sql
WHERE writer_count <= MAX_TABLE_FANOUT
```

Tables written by more than `MAX_TABLE_FANOUT` distinct producers are not included in dependency edges. On a large demo workspace (e2-demo-field-eng), hub tables (platform monitoring sinks + user scratch tables) alone created 872,990 edges (a writers × readers cartesian product). Capping at 20 reduces this to ~7,900 edges while excluding only 46 of ~30,600 tables. Excluded tables are reported as `SHARED_TABLE` records, not hidden, because "795 jobs write one table" is itself an insight.

### Edge deduplication and consumer filtering

Consumer kinds are filtered to a configurable set (default: `JOB,PIPELINE,DASHBOARD,QUERY,GENIE,ALERT`). Edges are deduplicated on `(producer_kind, producer_key, consumer_kind, consumer_key)` and parallel edge kinds are merged into an array.

---

## Layer 2: Graph Table (DAG)

**File:** `src/job/jiig-dag-table-creation.sql`

**Input:** `lineage_edges` + system.lakeflow metadata tables

**Output:** `dag_relationships` table

### Node metadata

Nodes are constructed from:

1. **Jobs** from `system.lakeflow.jobs` (latest SCD2 row, filtered for `delete_time IS NULL` or failed jobs kept for forensics)
2. **Pipelines** from `system.lakeflow.pipelines` (latest SCD2 row)
3. **Leaf consumers** (dashboards, Genie spaces, queries, alerts) inferred from `lineage_edges`

Nodes tagged `LakehouseMonitoringAnomalyDetection` are excluded (Databricks-managed monitoring noise).

### Failure detection

Failures within `failure_lookback_days` are detected:

- **Jobs:** `system.lakeflow.job_run_timeline` rows with `result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')`
- **Pipelines:** `system.lakeflow.pipeline_update_timeline` rows with `result_state = 'FAILED'` and `update_type IN ('REFRESH', 'FULL_REFRESH')`

Counts use `COUNT(DISTINCT run_id / update_id)` (pre-2.0 used `COUNT(*)`, which over-counted because one run occupies multiple timeline rows).

### Output schema

| Column | Type | Purpose |
|---|---|---|
| `node_type` | STRING | `"NODE"` or `"EDGE"` |
| `id` | STRING | Entity ID |
| `kind` | STRING | `"JOB"`, `"PIPELINE"`, `"DASHBOARD"`, `"QUERY"`, `"GENIE"`, or `"ALERT"` |
| `name` | STRING | Display name (nullable for leaf consumers) |
| `description` | STRING | Job/Pipeline description (nullable) |
| `owner_email` | STRING | Creator or run-as email (nullable; can be numeric ID if not backfilled) |
| `in_degree` | INT | Number of incoming edges |
| `out_degree` | INT | Number of outgoing edges |
| `downstream_reach` | INT | Number of distinct entities reachable via downstream edges (within `impact_max_depth` hops) |
| `failure_count` | INT | Number of distinct failures in window |
| `last_failed_time` | TIMESTAMP | Most recent failure timestamp |
| `failure_detail` | STRING | Termination code or update details |
| **For `node_type = "EDGE"`:** | | |
| `from_kind` | STRING | Producer kind |
| `from_id` | STRING | Producer ID |
| `to_kind` | STRING | Consumer kind |
| `to_id` | STRING | Consumer ID |
| `type` | ARRAY<STRING> | Edge kinds (merged if parallel) |
| `connecting_tables` | ARRAY<STRING> | Tables forming the dependency |

### Downstream reach computation

Pre-2.0 attempted to compute reachability via `WITH RECURSIVE` with path tracking. On a large workspace with ~3,600 nodes, the recursion exploded beyond the 1,000,000-row limit. Version 2.0 uses fixed-depth joins (iterating up to `impact_max_depth` times), which is:

1. More tractable at scale
2. Sufficient for pre-computing the insight that "this failed node affects X downstream entities"

The impact table (see Layer 3) still uses recursion because it only traces from the small subset of failed nodes and uses looser path semantics (no path deduplication, just shortest hop count).

---

## Layer 3: Dashboard Impact Table

**File:** `src/job/jiig-dashboard-table-creation.sql`

**Input:** `lineage_edges` + system.lakeflow metadata tables

**Output:** `jiig_dashboard` table

### Purpose

For every entity that failed in the window, compute all downstream entities reachable within `impact_max_depth` hops, with the shortest path distance and a sample path.

### Output schema

| Column | Type | Purpose |
|---|---|---|
| `failed_entity_kind` | STRING | `"JOB"` or `"PIPELINE"` (only producers can fail) |
| `failed_entity_id` | STRING | Failed entity ID |
| `failed_entity_name` | STRING | Display name |
| `failure_count` | INT | `COUNT(DISTINCT run_id / update_id)` |
| `last_failed_time` | TIMESTAMP | Most recent failure |
| `failure_detail` | STRING | Termination code or update details |
| `affected_entity_kind` | STRING | Consumer kind |
| `affected_entity_id` | STRING | Consumer ID |
| `affected_entity_name` | STRING | Display name |
| `affected_owner_email` | STRING | Owner email (nullable) |
| `hop_distance` | INT | Shortest number of edges to reach affected from failed |
| `impact_path` | STRING | Sample path (e.g., `Job 123 → Pipeline 456 → Dashboard 789`) |

### Recursion approach

Unlike the graph table, this query uses `WITH RECURSIVE` over the shared edges, starting from failed nodes. Because:

1. Only a few hundred nodes fail (out of 3,600+)
2. Recursion stops at `impact_max_depth` hops

The recursion remains tractable and yields shortest-path semantics.

### Path format

`impact_path` is a string like `Job 123 → Pipeline 456 → Dashboard 789`, built by concatenating `kind id` with arrows. This is human-readable and sufficient for notifying owners ("your pipeline is affected by this job failure through this chain").

---

## Dependency resolution order

The job's task sequence in `resources/jiig.job.yml`:

1. **jiig_lineage_edges** (no dependencies) — builds shared edge layer
2. **jiig_dag_table** (depends on #1) — builds graph table
3. **jiig_dashboard_table** (depends on #1) — builds impact table

Tasks 2 and 3 are independent; they can run in parallel once #1 completes.

---

## Extending the design

### Adding a new consumer kind

To include a new entity type (e.g., `MODEL`, `SERVING_ENDPOINT`):

1. Modify `lineage_edges.sql`:
   - Add the kind to the `CASE` statement in `lineage_raw`
   - Extract the entity ID from `entity_metadata`
   - Add to `allowed_consumer_kinds` (or hardcode it in the array)

2. Modify `dag_table_creation.sql` (if the entity can fail):
   - Add a new `failed_<kind>` CTE that joins to the entity's failure timeline
   - Union it into `failed_entities`

3. No changes needed to `dashboard_table_creation.sql` (it reads from shared edges).

### Changing edge semantics

To exclude a certain edge kind or add a new one:

1. Modify the edge extraction logic in `lineage_edges.sql`
2. Update the correctness rules in this document
3. Modify downstream consumers (`dag_table_creation.sql`, `dashboard_table_creation.sql`) if the new kind requires special handling (e.g., "only count DEPENDENCY, not TRIGGER when computing reachability")

### Tuning recursion limits

If you see "exceeded recursion row limit" errors:

- Lower `impact_max_depth` to reduce path length
- Or modify `dag_table_creation.sql` to use fixed-depth joins for a subset of nodes (e.g., only the top 500 by degree)

---

## Original requirements note (Korean)

```
Lakeflow Job의 fail로 인하여 발생할 수 있는 문제를 쉽게 파악하기 위한 코드 베이스임
Job A가 table1로 부터 table2, table3을 만든다고 가정할 때 table2 혹은 table3을 소스로 사용하는 Job B를 찾아내야 함
이것을 table lineage 관계를 파악하여 구현. 
최초 만들 당시와 현재는 데이터브릭스가 제공하는 시스템 테이블이 변경되었기 때문에 최신화 해야함
Notebook Job뿐만 아니라 SDP Job에서 refresh 역시도 영향 범위를 파악해야 함
Graph로 표현하기 위한 App에서 기존에는 scalable 문제가 있었는데, 이를 사용성과 정보성, 그리고 Job들간의 관계에서 insight를 추출한다는 측면에서 전면 재 수정이 필요함
```
