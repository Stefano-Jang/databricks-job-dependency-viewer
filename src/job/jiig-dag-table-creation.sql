-- =====================================================================
-- Graph table: nodes + edges for the JIIG app
--
-- Edges come from the shared lineage layer (jiig-lineage-edges.sql), so this
-- table and the impact table always agree on what a dependency is.
--
-- Node metadata sources:
--   * system.lakeflow.jobs / pipelines              -- SCD2, latest row
--   * system.lakeflow.job_run_timeline              -- job failure / activity
--   * system.lakeflow.pipeline_update_timeline      -- pipeline update failure
--   * the shared edge table                         -- leaf consumers (dashboards,
--                                                      Genie spaces, queries, alerts)
--
-- Per-node graph metrics (in_degree / out_degree / downstream_reach /
-- criticality_rank) are computed here rather than in the app, so the app never
-- has to load the whole graph to answer "which job matters most".
-- =====================================================================
DECLARE OR REPLACE TARGET_WORKSPACE_ID   STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DAG_TBL               STRING DEFAULT {{jiig_dag_table}};
DECLARE OR REPLACE EDGE_TBL              STRING DEFAULT {{lineage_edge_table}};
-- Failures/activity inside this window mark a node FAILED / recently active
DECLARE OR REPLACE FAILURE_LOOKBACK_DAYS INT    DEFAULT CAST({{failure_lookback_days}} AS INT);
-- Hops to follow when precomputing downstream_reach
DECLARE OR REPLACE IMPACT_MAX_DEPTH      INT    DEFAULT CAST({{impact_max_depth}} AS INT);
-- The reach join chain below is unrolled to 5 levels, so a larger
-- IMPACT_MAX_DEPTH cannot be honoured here; clamp instead of truncating quietly.
DECLARE OR REPLACE REACH_DEPTH_LIMIT     INT    DEFAULT 5;
DECLARE OR REPLACE REACH_DEPTH           INT    DEFAULT least(IMPACT_MAX_DEPTH, REACH_DEPTH_LIMIT);

DECLARE OR REPLACE TARGET_SCHEMA STRING DEFAULT regexp_extract(DAG_TBL, '^(.*)\\.[^.]+$', 1);
CREATE SCHEMA IF NOT EXISTS identifier(TARGET_SCHEMA);

CREATE OR REPLACE TABLE identifier(DAG_TBL) AS (
WITH time_bounds AS (
  SELECT timestampadd(DAY, -FAILURE_LOOKBACK_DAYS, current_timestamp()) AS failure_since_ts
),

edges_src AS (
  SELECT producer_kind, producer_key, consumer_kind, consumer_key,
         edge_kinds, edge_tables, edge_table_count,
         producer_lineage_owner, consumer_lineage_owner
  FROM identifier(EDGE_TBL)
  WHERE result_type = 'EDGE'
),

shared_tbls AS (
  SELECT table_full_name, writer_count
  FROM identifier(EDGE_TBL)
  WHERE result_type = 'SHARED_TABLE'
),

-- ============================================
-- 1) Latest (SCD2) entity metadata
-- ============================================
-- Latest row regardless of deletion: failed entities stay visible even when
-- deleted after the failure (forensics); healthy nodes must still exist.
-- The LakehouseMonitoringAnomalyDetection tag marks Databricks-managed
-- monitoring entities, which are noise here. It is applied to both jobs and
-- pipelines -- pre-2.0 filtered only jobs, so monitoring pipelines leaked in.
jobs_latest AS (
  SELECT * FROM (
    SELECT
      job_id, name, description, creator_id, creator_user_name,
      run_as, run_as_user_name, tags, delete_time, create_time, change_time
    FROM system.lakeflow.jobs
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
  )
  WHERE COALESCE(NOT array_contains(map_keys(tags), 'LakehouseMonitoringAnomalyDetection'), true)
),

pipelines_latest AS (
  SELECT * FROM (
    SELECT
      pipeline_id, name, pipeline_type, created_by, run_as, tags, settings,
      delete_time, create_time, change_time
    FROM system.lakeflow.pipelines
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
  )
  WHERE COALESCE(NOT array_contains(map_keys(tags), 'LakehouseMonitoringAnomalyDetection'), true)
),

-- ============================================
-- 2) Failures within the window
-- ============================================
-- COUNT(DISTINCT run_id/update_id), not COUNT(*): the timeline tables can hold
-- several rows for one run, which inflated failure_count before 2.0.
failed_jobs AS (
  SELECT
    jt.job_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_failed_time,
    MIN(COALESCE(jt.period_end_time, jt.period_start_time)) AS first_failed_time,
    COUNT(DISTINCT jt.run_id)                               AS failure_count,
    max_by(jt.termination_code, COALESCE(jt.period_end_time, jt.period_start_time)) AS failure_detail
  FROM system.lakeflow.job_run_timeline jt
  CROSS JOIN time_bounds t
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND jt.result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.failure_since_ts
  GROUP BY jt.job_id
),

failed_pipelines AS (
  SELECT
    put.pipeline_id,
    MAX(COALESCE(put.period_end_time, put.period_start_time)) AS last_failed_time,
    MIN(COALESCE(put.period_end_time, put.period_start_time)) AS first_failed_time,
    COUNT(DISTINCT put.update_id)                             AS failure_count,
    max_by(CONCAT('update_type=', put.update_type, ', update_id=', put.update_id),
           COALESCE(put.period_end_time, put.period_start_time)) AS failure_detail
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND put.result_state = 'FAILED'
    AND put.update_type IN ('REFRESH', 'FULL_REFRESH')
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.failure_since_ts
  GROUP BY put.pipeline_id
),

-- ============================================
-- 3) Recent activity within the window
-- ============================================
job_activity AS (
  SELECT jt.job_id, MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_activity_time
  FROM system.lakeflow.job_run_timeline jt
  CROSS JOIN time_bounds t
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.failure_since_ts
  GROUP BY jt.job_id
),

pipeline_activity AS (
  SELECT put.pipeline_id, MAX(COALESCE(put.period_end_time, put.period_start_time)) AS last_activity_time
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.failure_since_ts
  GROUP BY put.pipeline_id
),

-- ============================================
-- 4) Node universe: failed, recently active, or touched by an edge
-- ============================================
candidate_entities AS (
  SELECT 'JOB' AS entity_kind, job_id AS entity_key FROM failed_jobs
  UNION SELECT 'JOB', job_id FROM job_activity
  UNION SELECT 'PIPELINE', pipeline_id FROM failed_pipelines
  UNION SELECT 'PIPELINE', pipeline_id FROM pipeline_activity
  UNION SELECT producer_kind, producer_key FROM edges_src
  UNION SELECT consumer_kind, consumer_key FROM edges_src
),

job_nodes AS (
  SELECT
    'JOB'                                          AS node_kind,
    c.entity_key                                   AS node_id,
    'job'                                          AS node_type,
    CAST(NULL AS STRING)                           AS node_subtype,
    CONCAT(j.name, CASE WHEN j.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END) AS node_name,
    j.description                                  AS node_description,
    COALESCE(j.creator_user_name, j.creator_id)    AS node_creator_email,
    COALESCE(j.run_as_user_name, j.run_as)         AS node_run_as_email,
    fj.job_id IS NOT NULL                          AS node_is_failed,
    fj.last_failed_time                            AS node_last_failed_time,
    fj.first_failed_time                           AS node_first_failed_time,
    COALESCE(fj.failure_count, 0)                  AS node_failure_count,
    fj.failure_detail                              AS node_failure_detail,
    ja.last_activity_time                          AS node_last_activity_time,
    COALESCE(j.create_time, j.change_time)         AS node_created_time
  FROM candidate_entities c
  JOIN jobs_latest j ON c.entity_kind = 'JOB' AND c.entity_key = j.job_id
  LEFT JOIN failed_jobs fj  ON fj.job_id = j.job_id
  LEFT JOIN job_activity ja ON ja.job_id = j.job_id
  WHERE j.delete_time IS NULL OR fj.job_id IS NOT NULL
),

-- pipeline_type distinguishes what users actually see: MATERIALIZED_VIEW and
-- STREAMING_TABLE appear as tables in the UI, not as pipelines. Keeping the
-- subtype lets the app label them honestly.
pipeline_nodes AS (
  SELECT
    'PIPELINE'                                     AS node_kind,
    c.entity_key                                   AS node_id,
    'pipeline'                                     AS node_type,
    p.pipeline_type                                AS node_subtype,
    CONCAT(p.name, CASE WHEN p.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END) AS node_name,
    CONCAT_WS(' · ',
      p.pipeline_type,
      CASE WHEN p.settings.serverless THEN 'serverless' END,
      CASE WHEN p.settings.continuous THEN 'continuous' END)  AS node_description,
    p.created_by                                   AS node_creator_email,
    p.run_as                                       AS node_run_as_email,
    fp.pipeline_id IS NOT NULL                     AS node_is_failed,
    fp.last_failed_time                            AS node_last_failed_time,
    fp.first_failed_time                           AS node_first_failed_time,
    COALESCE(fp.failure_count, 0)                  AS node_failure_count,
    fp.failure_detail                              AS node_failure_detail,
    pa.last_activity_time                          AS node_last_activity_time,
    COALESCE(p.create_time, p.change_time)         AS node_created_time
  FROM candidate_entities c
  JOIN pipelines_latest p ON c.entity_kind = 'PIPELINE' AND c.entity_key = p.pipeline_id
  LEFT JOIN failed_pipelines fp  ON fp.pipeline_id = p.pipeline_id
  LEFT JOIN pipeline_activity pa ON pa.pipeline_id = p.pipeline_id
  WHERE p.delete_time IS NULL OR fp.pipeline_id IS NOT NULL
),

-- Endpoints with no row in system.lakeflow.jobs / pipelines still need a node,
-- otherwise their edges are silently dropped and the blast radius is
-- understated. Two distinct cases, both real and both measured on a demo
-- workspace:
--   * Leaf consumers (dashboards, Genie spaces, queries, alerts) have no
--     system table at all -- they are read-only and never fail on their own.
--     A broken job here means a stale dashboard someone is reading right now.
--   * Jobs/pipelines missing from the SCD2 metadata: 905 of 1,491 job
--     producers were absent from system.lakeflow.jobs, yet 751 of those had
--     run history, so they are real. Alert-backed jobs never appear there at
--     all. Dropping them cost ~86% of the edges.
-- Owner comes from table_lineage.created_by, which is populated for these.
edge_endpoints AS (
  SELECT producer_kind AS node_kind, producer_key AS node_id, producer_lineage_owner AS owner_email
  FROM edges_src
  UNION ALL
  SELECT consumer_kind, consumer_key, consumer_lineage_owner FROM edges_src
),

unresolved_nodes AS (
  SELECT
    ep.node_kind,
    ep.node_id,
    lower(ep.node_kind)                            AS node_type,
    CASE WHEN ep.node_kind IN ('JOB', 'PIPELINE') THEN 'UNREGISTERED' END AS node_subtype,
    CASE
      WHEN ep.node_kind IN ('JOB', 'PIPELINE')
        THEN CONCAT(initcap(lower(ep.node_kind)), ' ', ep.node_id)
      ELSE CONCAT(initcap(lower(ep.node_kind)), ' ', substr(ep.node_id, 1, 8))
    END                                            AS node_name,
    CASE WHEN ep.node_kind IN ('JOB', 'PIPELINE')
         THEN 'Seen in lineage only (no current metadata row)' END AS node_description,
    max_by(ep.owner_email, ep.owner_email IS NOT NULL) AS node_creator_email,
    max_by(ep.owner_email, ep.owner_email IS NOT NULL) AS node_run_as_email,
    false                                          AS node_is_failed,
    CAST(NULL AS TIMESTAMP)                        AS node_last_failed_time,
    CAST(NULL AS TIMESTAMP)                        AS node_first_failed_time,
    0                                              AS node_failure_count,
    CAST(NULL AS STRING)                           AS node_failure_detail,
    CAST(NULL AS TIMESTAMP)                        AS node_last_activity_time,
    CAST(NULL AS TIMESTAMP)                        AS node_created_time
  FROM edge_endpoints ep
  LEFT JOIN job_nodes      jn ON jn.node_kind = ep.node_kind AND jn.node_id = ep.node_id
  LEFT JOIN pipeline_nodes pn ON pn.node_kind = ep.node_kind AND pn.node_id = ep.node_id
  WHERE jn.node_id IS NULL AND pn.node_id IS NULL
  GROUP BY ep.node_kind, ep.node_id
),

all_nodes AS (
  SELECT * FROM job_nodes
  UNION ALL SELECT * FROM pipeline_nodes
  UNION ALL SELECT * FROM unresolved_nodes
),

-- Keep only edges whose endpoints survived the metadata join. Matching on
-- (kind, key) rather than key alone, so a job id can never bind to a pipeline.
valid_edges AS (
  SELECT e.*
  FROM edges_src e
  JOIN all_nodes ns ON ns.node_kind = e.producer_kind AND ns.node_id = e.producer_key
  JOIN all_nodes nt ON nt.node_kind = e.consumer_kind AND nt.node_id = e.consumer_key
),

-- ============================================
-- 5) Graph metrics, precomputed
-- ============================================
-- downstream_reach is the transitive consumer count up to IMPACT_MAX_DEPTH,
-- expanded as fixed-depth joins instead of WITH RECURSIVE: path-tracking
-- recursion over every node blows the 1M recursion-row limit on a large
-- workspace, while bounded joins stay well inside it.
--
-- The chain is unrolled to REACH_DEPTH_LIMIT (5) levels. IMPACT_MAX_DEPTH above
-- that would silently truncate reach and therefore mis-rank criticality, so it
-- is clamped here and the clamp is visible in the output rather than implicit.
-- Raising the limit means adding h6/h7 CTEs below, not just changing this value.
e AS (
  SELECT DISTINCT
    concat_ws(':', producer_kind, producer_key) AS src,
    concat_ws(':', consumer_kind, consumer_key) AS dst
  FROM valid_edges
),
h1 AS (SELECT src AS root, dst AS node FROM e),
h2 AS (SELECT DISTINCT a.root, x.dst AS node FROM h1 a JOIN e x ON x.src = a.node
       WHERE REACH_DEPTH >= 2 AND x.dst <> a.root),
h3 AS (SELECT DISTINCT a.root, x.dst AS node FROM h2 a JOIN e x ON x.src = a.node
       WHERE REACH_DEPTH >= 3 AND x.dst <> a.root),
h4 AS (SELECT DISTINCT a.root, x.dst AS node FROM h3 a JOIN e x ON x.src = a.node
       WHERE REACH_DEPTH >= 4 AND x.dst <> a.root),
h5 AS (SELECT DISTINCT a.root, x.dst AS node FROM h4 a JOIN e x ON x.src = a.node
       WHERE REACH_DEPTH >= 5 AND x.dst <> a.root),

reach AS (
  SELECT root, COUNT(DISTINCT node) AS downstream_reach
  FROM (
    SELECT * FROM h1 UNION ALL SELECT * FROM h2 UNION ALL SELECT * FROM h3
    UNION ALL SELECT * FROM h4 UNION ALL SELECT * FROM h5
  )
  GROUP BY root
),

degrees AS (
  SELECT node_key,
         SUM(out_d) AS out_degree,
         SUM(in_d)  AS in_degree
  FROM (
    SELECT concat_ws(':', producer_kind, producer_key) AS node_key, 1 AS out_d, 0 AS in_d FROM valid_edges
    UNION ALL
    SELECT concat_ws(':', consumer_kind, consumer_key) AS node_key, 0, 1 FROM valid_edges
  )
  GROUP BY node_key
),

node_metrics AS (
  SELECT
    n.*,
    concat_ws(':', n.node_kind, n.node_id)      AS node_key,
    COALESCE(d.in_degree, 0)                    AS in_degree,
    COALESCE(d.out_degree, 0)                   AS out_degree,
    COALESCE(r.downstream_reach, 0)             AS downstream_reach
  FROM all_nodes n
  LEFT JOIN degrees d ON d.node_key = concat_ws(':', n.node_kind, n.node_id)
  LEFT JOIN reach   r ON r.root     = concat_ws(':', n.node_kind, n.node_id)
)

-- ============================================
-- 6) Final union: NODES + EDGES + SHARED_TABLES
-- ============================================
SELECT
  'NODES'                       AS result_type,
  node_id                       AS id,
  node_type                     AS type,
  node_subtype                  AS subtype,
  node_name                     AS name,
  node_description              AS description,
  node_creator_email            AS creator_email,
  node_run_as_email             AS run_as_email,
  node_is_failed                AS is_failed,
  node_last_failed_time         AS last_failed_time,
  node_first_failed_time        AS first_failed_time,
  node_failure_count            AS failure_count,
  node_failure_detail           AS failure_detail,
  node_last_activity_time       AS last_activity_time,
  node_created_time             AS created_time,
  CASE WHEN node_is_failed THEN 'FAILED' ELSE 'HEALTHY' END AS status,
  in_degree,
  out_degree,
  downstream_reach,
  -- 1 = most critical. Ranks by blast radius, then direct consumers.
  CAST(RANK() OVER (ORDER BY downstream_reach DESC, out_degree DESC) AS INT) AS criticality_rank,
  CAST(NULL AS STRING)          AS source_id,
  CAST(NULL AS STRING)          AS target_id,
  CAST(NULL AS STRING)          AS connecting_tables,
  CAST(NULL AS INT)             AS edge_table_count,
  CAST(NULL AS ARRAY<STRING>)   AS edge_kinds,
  CAST(NULL AS INT)             AS writer_count
FROM node_metrics

UNION ALL

SELECT
  'EDGES'                       AS result_type,
  CONCAT(producer_key, '->', consumer_key) AS id,
  lower(concat_ws('+', edge_kinds))        AS type,
  CAST(NULL AS STRING)          AS subtype,
  CONCAT(producer_key, ' -> ', consumer_key) AS name,
  CASE WHEN size(edge_tables) = 0
       THEN 'Orchestration edge'
       ELSE CONCAT('Tables: ', concat_ws(', ', edge_tables)) END AS description,
  CAST(NULL AS STRING)          AS creator_email,
  CAST(NULL AS STRING)          AS run_as_email,
  CAST(NULL AS BOOLEAN)         AS is_failed,
  CAST(NULL AS TIMESTAMP)       AS last_failed_time,
  CAST(NULL AS TIMESTAMP)       AS first_failed_time,
  CAST(NULL AS BIGINT)          AS failure_count,
  CAST(NULL AS STRING)          AS failure_detail,
  CAST(NULL AS TIMESTAMP)       AS last_activity_time,
  CAST(NULL AS TIMESTAMP)       AS created_time,
  'ACTIVE'                      AS status,
  CAST(NULL AS BIGINT)          AS in_degree,
  CAST(NULL AS BIGINT)          AS out_degree,
  CAST(NULL AS BIGINT)          AS downstream_reach,
  CAST(NULL AS INT)             AS criticality_rank,
  producer_key                  AS source_id,
  consumer_key                  AS target_id,
  concat_ws(', ', edge_tables)  AS connecting_tables,
  edge_table_count,
  edge_kinds,
  CAST(NULL AS INT)             AS writer_count
FROM valid_edges

UNION ALL

-- Shared sinks: excluded from dependency edges (see jiig-lineage-edges.sql) but
-- carried through because "300 jobs write this one table" is worth surfacing.
SELECT
  'SHARED_TABLES'               AS result_type,
  table_full_name               AS id,
  'table'                       AS type,
  CAST(NULL AS STRING)          AS subtype,
  table_full_name               AS name,
  CONCAT(writer_count, ' distinct producers write this table') AS description,
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS BOOLEAN),
  CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP), CAST(NULL AS BIGINT),
  CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP),
  'SHARED'                      AS status,
  CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS INT),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT),
  CAST(NULL AS ARRAY<STRING>),
  writer_count
FROM shared_tbls
);
