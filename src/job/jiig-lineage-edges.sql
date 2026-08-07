-- =====================================================================
-- Shared dependency-edge layer (single source of truth for JIIG)
--
-- Both the graph table (jiig-dag-table-creation.sql) and the impact table
-- (jiig-dashboard-table-creation.sql) read this table, so the two can never
-- disagree about what an edge is.
--
-- An edge means "if the producer breaks, the consumer is affected":
--   DEPENDENCY    producer writes a table the consumer reads
--   TRIGGER       a job task starts a pipeline update
--   TABLE_TRIGGER a job declares a table-update trigger on a table someone writes
--
-- Sources (native system tables only):
--   * system.access.table_lineage              -- table-level edges
--   * system.lakeflow.pipeline_update_timeline -- job -> pipeline triggers
--   * system.lakeflow.jobs                     -- table-update triggers
--
-- Two correctness rules that the pre-2.0 queries missed:
--   1. Only direct_access lineage is a real edge. direct_access = false rows
--      are transitive ancestors; treating them as direct edges invents
--      dependencies that do not exist.
--   2. Tables written by very many unrelated producers (platform/monitoring
--      sinks such as system.data_quality_monitoring.table_results, but also
--      user-catalog scratch tables) are not dependencies -- they are shared
--      sinks. Joining writers x readers on them is a cartesian product: on a
--      large demo workspace they alone inflated the edge set from ~6k to
--      ~873k. Tables whose writer count exceeds MAX_TABLE_FANOUT are excluded
--      from dependency edges and reported separately as SHARED_TABLES.
-- =====================================================================
DECLARE OR REPLACE TARGET_WORKSPACE_ID   STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE EDGE_TBL              STRING DEFAULT {{lineage_edge_table}};
DECLARE OR REPLACE LINEAGE_LOOKBACK_DAYS INT    DEFAULT CAST({{lineage_lookback_days}} AS INT);
-- A table written by more than this many distinct producers is treated as a
-- shared sink, not a dependency. Raise it to keep more edges, lower it for a
-- tighter graph.
DECLARE OR REPLACE MAX_TABLE_FANOUT      INT    DEFAULT CAST({{max_table_fanout}} AS INT);
-- Consumer kinds to include beyond JOB/PIPELINE. Comma-separated subset of
-- DASHBOARD, QUERY, GENIE, ALERT. These are read-only leaves: they consume
-- tables but never produce them, so they end a path rather than extend it.
DECLARE OR REPLACE CONSUMER_KINDS        STRING DEFAULT {{include_consumer_kinds}};

DECLARE OR REPLACE TARGET_SCHEMA STRING DEFAULT regexp_extract(EDGE_TBL, '^(.*)\\.[^.]+$', 1);
CREATE SCHEMA IF NOT EXISTS identifier(TARGET_SCHEMA);

CREATE OR REPLACE TABLE identifier(EDGE_TBL) AS (
WITH time_bounds AS (
  SELECT
    timestampadd(DAY, -LINEAGE_LOOKBACK_DAYS, current_timestamp()) AS lineage_since_ts,
    date_sub(current_date(), LINEAGE_LOOKBACK_DAYS)                AS lineage_since_date
),

allowed_consumer_kinds AS (
  SELECT array_union(
           array('JOB', 'PIPELINE'),
           transform(split(coalesce(CONSUMER_KINDS, ''), '\\s*,\\s*'), x -> upper(trim(x)))
         ) AS kinds
),

-- ============================================
-- 1) Lineage, resolved to an entity kind + key
-- ============================================
-- Resolution is by entity_metadata field, NOT by entity_type. Measured on a
-- demo workspace: Genie rows carry entity_type = NULL, and every alert row
-- also carries a job_id (alerts execute as hidden jobs that do not appear in
-- system.lakeflow.jobs at all). Ordering alert/genie ahead of job_info keeps
-- those consumers attributable instead of silently dropped.
lineage_raw AS (
  SELECT
    CASE
      WHEN l.entity_metadata.alert_id           IS NOT NULL THEN 'ALERT'
      WHEN l.entity_metadata.genie_space_id     IS NOT NULL THEN 'GENIE'
      WHEN l.entity_metadata.dashboard_id       IS NOT NULL THEN 'DASHBOARD'
      WHEN l.entity_metadata.job_info.job_id    IS NOT NULL THEN 'JOB'
      WHEN l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id IS NOT NULL THEN 'PIPELINE'
      WHEN l.entity_metadata.sql_query_id       IS NOT NULL THEN 'QUERY'
    END AS entity_kind,
    COALESCE(
      l.entity_metadata.alert_id,
      l.entity_metadata.genie_space_id,
      l.entity_metadata.dashboard_id,
      l.entity_metadata.job_info.job_id,
      l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id,
      l.entity_metadata.sql_query_id
    ) AS entity_key,
    l.created_by,
    l.source_table_full_name,
    l.target_table_full_name
  FROM system.access.table_lineage l
  CROSS JOIN time_bounds t
  WHERE l.workspace_id = TARGET_WORKSPACE_ID
    AND l.event_date >= t.lineage_since_date
    -- Rule 1: only direct dependencies are edges.
    AND l.direct_access
),

lineage AS (
  SELECT r.*
  FROM lineage_raw r
  CROSS JOIN allowed_consumer_kinds a
  WHERE r.entity_kind IS NOT NULL
    AND r.entity_key IS NOT NULL
    AND array_contains(a.kinds, r.entity_kind)
),

-- Owner of each entity as seen by lineage. Used for consumer kinds that have
-- no system table of their own (dashboards, queries, Genie spaces, alerts).
lineage_owners AS (
  SELECT entity_kind, entity_key,
         max_by(created_by, created_by IS NOT NULL) AS lineage_created_by
  FROM lineage
  GROUP BY entity_kind, entity_key
),

-- Only JOB/PIPELINE can produce a table. Everything else is a leaf consumer.
entity_writes AS (
  SELECT DISTINCT entity_kind, entity_key, target_table_full_name AS table_full_name
  FROM lineage
  WHERE target_table_full_name IS NOT NULL
    AND entity_kind IN ('JOB', 'PIPELINE')
),

entity_reads AS (
  SELECT DISTINCT entity_kind, entity_key, source_table_full_name AS table_full_name
  FROM lineage
  WHERE source_table_full_name IS NOT NULL
),

-- ============================================
-- 2) Rule 2: separate real tables from shared sinks
-- ============================================
table_fanout AS (
  SELECT table_full_name, COUNT(DISTINCT concat_ws(':', entity_kind, entity_key)) AS writer_count
  FROM entity_writes
  GROUP BY table_full_name
),

dependency_tables AS (
  SELECT table_full_name, writer_count FROM table_fanout WHERE writer_count <= MAX_TABLE_FANOUT
),

shared_tables AS (
  SELECT table_full_name, writer_count FROM table_fanout WHERE writer_count > MAX_TABLE_FANOUT
),

-- ============================================
-- 3) Edges
-- ============================================
table_edges AS (
  SELECT
    w.entity_kind AS producer_kind,
    w.entity_key  AS producer_key,
    r.entity_kind AS consumer_kind,
    r.entity_key  AS consumer_key,
    'DEPENDENCY'  AS edge_kind,
    sort_array(collect_set(w.table_full_name)) AS edge_tables
  FROM entity_writes w
  JOIN dependency_tables d ON d.table_full_name = w.table_full_name
  JOIN entity_reads r      ON r.table_full_name = w.table_full_name
  WHERE NOT (w.entity_kind = r.entity_kind AND w.entity_key = r.entity_key)
  GROUP BY 1, 2, 3, 4, 5
),

-- Orchestration: a job task started a pipeline update.
-- job_id here is not always an id: managed refreshes report sentinels such as
-- 'SQL_SCHEDULE' (measured: 92k updates across 202 pipelines under one such
-- value). Keeping them would invent a single fake job that appears to own
-- hundreds of pipelines and would top the criticality ranking, so only
-- numeric ids are accepted.
trigger_edges AS (
  SELECT DISTINCT
    'JOB'      AS producer_kind,
    put.trigger_details.job_task.job_id AS producer_key,
    'PIPELINE' AS consumer_kind,
    put.pipeline_id AS consumer_key,
    'TRIGGER'  AS edge_kind,
    CAST(array() AS ARRAY<STRING>) AS edge_tables
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND put.trigger_details.job_task.job_id RLIKE '^[0-9]+$'
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.lineage_since_ts
),

-- Declared dependency: a job fires when a given table is updated, so whoever
-- writes that table is upstream of the job. This is stated in the job
-- definition rather than inferred from lineage.
table_update_triggers AS (
  SELECT job_id, explode(trigger.table_update.table_names) AS table_full_name
  FROM (
    SELECT job_id, trigger, delete_time
    FROM system.lakeflow.jobs
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
  )
  WHERE delete_time IS NULL
    AND trigger.table_update.table_names IS NOT NULL
),

table_trigger_edges AS (
  SELECT
    w.entity_kind   AS producer_kind,
    w.entity_key    AS producer_key,
    'JOB'           AS consumer_kind,
    tut.job_id      AS consumer_key,
    'TABLE_TRIGGER' AS edge_kind,
    sort_array(collect_set(w.table_full_name)) AS edge_tables
  FROM table_update_triggers tut
  JOIN entity_writes w      ON w.table_full_name = tut.table_full_name
  JOIN dependency_tables d  ON d.table_full_name = tut.table_full_name
  WHERE NOT (w.entity_kind = 'JOB' AND w.entity_key = tut.job_id)
  GROUP BY 1, 2, 3, 4, 5
),

-- One row per (producer, consumer): parallel edge kinds are merged into
-- edge_kinds so downstream traversal cannot multiply paths.
merged_edges AS (
  SELECT
    producer_kind, producer_key, consumer_kind, consumer_key,
    sort_array(collect_set(edge_kind))                          AS edge_kinds,
    array_sort(array_distinct(flatten(collect_list(edge_tables)))) AS edge_tables
  FROM (
    SELECT * FROM table_edges
    UNION ALL SELECT * FROM trigger_edges
    UNION ALL SELECT * FROM table_trigger_edges
  )
  GROUP BY 1, 2, 3, 4
)

-- ============================================
-- 4) Output: EDGES + SHARED_TABLES in one table
-- ============================================
SELECT
  'EDGE'                    AS result_type,
  e.producer_kind,
  e.producer_key,
  e.consumer_kind,
  e.consumer_key,
  e.edge_kinds,
  e.edge_tables,
  size(e.edge_tables)       AS edge_table_count,
  pw.lineage_created_by     AS producer_lineage_owner,
  cw.lineage_created_by     AS consumer_lineage_owner,
  CAST(NULL AS STRING)      AS table_full_name,
  CAST(NULL AS INT)         AS writer_count
FROM merged_edges e
LEFT JOIN lineage_owners pw
  ON pw.entity_kind = e.producer_kind AND pw.entity_key = e.producer_key
LEFT JOIN lineage_owners cw
  ON cw.entity_kind = e.consumer_kind AND cw.entity_key = e.consumer_key

UNION ALL

-- Excluded hub tables are reported, not hidden: a table with hundreds of
-- writers is itself an insight (a shared sink or an over-loaded contract).
SELECT
  'SHARED_TABLE'            AS result_type,
  CAST(NULL AS STRING)      AS producer_kind,
  CAST(NULL AS STRING)      AS producer_key,
  CAST(NULL AS STRING)      AS consumer_kind,
  CAST(NULL AS STRING)      AS consumer_key,
  CAST(NULL AS ARRAY<STRING>) AS edge_kinds,
  CAST(NULL AS ARRAY<STRING>) AS edge_tables,
  CAST(NULL AS INT)         AS edge_table_count,
  CAST(NULL AS STRING)      AS producer_lineage_owner,
  CAST(NULL AS STRING)      AS consumer_lineage_owner,
  s.table_full_name,
  s.writer_count
FROM shared_tables s
);
