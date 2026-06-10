-- =====================================================================
-- Create and Refresh DAG Relationships Table (nodes & edges for the app)
--
-- Sources (all native system tables, no auxiliary jobs required):
--   * system.lakeflow.jobs / pipelines              -- SCD2, latest row used
--   * system.lakeflow.job_run_timeline              -- job failure / activity
--   * system.lakeflow.pipeline_update_timeline      -- pipeline (SDP) update
--                                                      failure / activity
--   * system.access.table_lineage                   -- table-level edges,
--                                                      entity_metadata based
-- =====================================================================
DECLARE OR REPLACE TARGET_WORKSPACE_ID   STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DAG_TBL               STRING DEFAULT {{jiig_dag_table}};
-- Failures/activity inside this window mark a node FAILED / recently active
DECLARE OR REPLACE FAILURE_LOOKBACK_DAYS INT    DEFAULT CAST({{failure_lookback_days}} AS INT);
-- Lineage inside this (longer) window defines the dependency edges
DECLARE OR REPLACE LINEAGE_LOOKBACK_DAYS INT    DEFAULT CAST({{lineage_lookback_days}} AS INT);

-- Bootstrap the target schema (catalog.schema part of the table name)
DECLARE OR REPLACE TARGET_SCHEMA STRING DEFAULT regexp_extract(DAG_TBL, '^(.*)\\.[^.]+$', 1);
CREATE SCHEMA IF NOT EXISTS identifier(TARGET_SCHEMA);

CREATE OR REPLACE TABLE identifier(DAG_TBL) AS (
WITH time_bounds AS (
  SELECT
    timestampadd(DAY, -FAILURE_LOOKBACK_DAYS, current_timestamp()) AS failure_since_ts,
    timestampadd(DAY, -LINEAGE_LOOKBACK_DAYS, current_timestamp()) AS lineage_since_ts,
    date_sub(current_date(), LINEAGE_LOOKBACK_DAYS)                AS lineage_since_date
),

-- ============================================
-- 1) Latest (SCD2) entity metadata
-- ============================================
-- Latest row regardless of deletion: failed entities stay visible even when
-- they were deleted after the failure (forensics); other nodes must be alive.
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
      pipeline_id, name, pipeline_type, created_by, run_as,
      delete_time, create_time, change_time
    FROM system.lakeflow.pipelines
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
  )
),

-- ============================================
-- 2) Failures within the window
--    Jobs: job_run_timeline terminal rows
--    Pipelines (SDP): pipeline_update_timeline terminal FAILED updates
-- ============================================
failed_jobs AS (
  SELECT
    jt.job_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_failed_time,
    MIN(COALESCE(jt.period_end_time, jt.period_start_time)) AS first_failed_time,
    COUNT(*)                                                AS failure_count,
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
    COUNT(*)                                                  AS failure_count,
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
  SELECT
    jt.job_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_activity_time
  FROM system.lakeflow.job_run_timeline jt
  CROSS JOIN time_bounds t
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.failure_since_ts
  GROUP BY jt.job_id
),

pipeline_activity AS (
  SELECT
    put.pipeline_id,
    MAX(COALESCE(put.period_end_time, put.period_start_time)) AS last_activity_time
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.failure_since_ts
  GROUP BY put.pipeline_id
),

-- ============================================
-- 4) Lineage (entity_metadata based) and edges
-- ============================================
lineage AS (
  SELECT
    CASE WHEN l.entity_metadata.job_info.job_id IS NOT NULL THEN 'JOB' ELSE 'PIPELINE' END AS entity_kind,
    COALESCE(l.entity_metadata.job_info.job_id,
             l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id) AS entity_key,
    l.source_table_full_name,
    l.target_table_full_name
  FROM system.access.table_lineage l
  CROSS JOIN time_bounds t
  WHERE l.workspace_id = TARGET_WORKSPACE_ID
    AND l.event_date >= t.lineage_since_date
    AND (l.entity_metadata.job_info.job_id IS NOT NULL
         OR l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id IS NOT NULL)
),

entity_writes AS (
  SELECT DISTINCT entity_kind, entity_key, target_table_full_name AS table_full_name
  FROM lineage
  WHERE target_table_full_name IS NOT NULL
),

entity_reads AS (
  SELECT DISTINCT entity_kind, entity_key, source_table_full_name AS table_full_name
  FROM lineage
  WHERE source_table_full_name IS NOT NULL
),

-- Table dependency: producer writes a table the consumer reads.
-- One row per (producer, consumer) pair with the connecting tables aggregated.
table_edges AS (
  SELECT
    w.entity_kind AS producer_kind,
    w.entity_key  AS producer_key,
    r.entity_kind AS consumer_kind,
    r.entity_key  AS consumer_key,
    concat_ws(', ', sort_array(collect_set(w.table_full_name))) AS edge_tables,
    COUNT(DISTINCT w.table_full_name)                           AS edge_table_count
  FROM entity_writes w
  JOIN entity_reads r
    ON w.table_full_name = r.table_full_name
  WHERE NOT (w.entity_kind = r.entity_kind AND w.entity_key = r.entity_key)
  GROUP BY 1, 2, 3, 4
),

-- Orchestration dependency: a job task triggered the pipeline update.
trigger_edges AS (
  SELECT DISTINCT
    'JOB'      AS producer_kind,
    put.trigger_details.job_task.job_id AS producer_key,
    'PIPELINE' AS consumer_kind,
    put.pipeline_id AS consumer_key
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND put.trigger_details.job_task.job_id IS NOT NULL
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.lineage_since_ts
),

all_edges AS (
  SELECT producer_kind, producer_key, consumer_kind, consumer_key,
         edge_tables, edge_table_count, 'DEPENDENCY' AS edge_kind
  FROM table_edges
  UNION ALL
  SELECT producer_kind, producer_key, consumer_kind, consumer_key,
         CAST(NULL AS STRING), 0, 'TRIGGER'
  FROM trigger_edges
),

-- ============================================
-- 5) Node universe: failed, recently active, or connected by an edge
-- ============================================
candidate_entities AS (
  SELECT 'JOB' AS entity_kind, job_id AS entity_key FROM failed_jobs
  UNION
  SELECT 'JOB', job_id FROM job_activity
  UNION
  SELECT 'PIPELINE', pipeline_id FROM failed_pipelines
  UNION
  SELECT 'PIPELINE', pipeline_id FROM pipeline_activity
  UNION
  SELECT producer_kind, producer_key FROM all_edges
  UNION
  SELECT consumer_kind, consumer_key FROM all_edges
),

job_nodes AS (
  SELECT
    c.entity_key                                   AS node_id,
    'job'                                          AS node_type,
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
  JOIN jobs_latest j
    ON c.entity_kind = 'JOB' AND c.entity_key = j.job_id
  LEFT JOIN failed_jobs fj  ON fj.job_id = j.job_id
  LEFT JOIN job_activity ja ON ja.job_id = j.job_id
  WHERE j.delete_time IS NULL OR fj.job_id IS NOT NULL
),

pipeline_nodes AS (
  SELECT
    c.entity_key                                   AS node_id,
    'pipeline'                                     AS node_type,
    CONCAT(p.name, CASE WHEN p.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END) AS node_name,
    p.pipeline_type                                AS node_description,
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
  JOIN pipelines_latest p
    ON c.entity_kind = 'PIPELINE' AND c.entity_key = p.pipeline_id
  LEFT JOIN failed_pipelines fp   ON fp.pipeline_id = p.pipeline_id
  LEFT JOIN pipeline_activity pa  ON pa.pipeline_id = p.pipeline_id
  WHERE p.delete_time IS NULL OR fp.pipeline_id IS NOT NULL
),

all_nodes AS (
  SELECT * FROM job_nodes
  UNION ALL
  SELECT * FROM pipeline_nodes
),

-- Keep only edges whose both endpoints survived the metadata join
valid_edges AS (
  SELECT e.*
  FROM all_edges e
  JOIN all_nodes ns ON ns.node_id = e.producer_key
  JOIN all_nodes nt ON nt.node_id = e.consumer_key
)

-- ============================================
-- 6) Final union: NODES + EDGES in a single table
-- ============================================
SELECT
  'NODES'                       AS result_type,
  node_id                       AS id,
  node_type                     AS type,
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
  upper(node_type)              AS label,
  CASE WHEN node_type = 'job'      THEN node_id END AS job_id,
  CASE WHEN node_type = 'pipeline' THEN node_id END AS pipeline_id,
  CAST(NULL AS STRING)          AS source_id,
  CAST(NULL AS STRING)          AS target_id,
  CAST(NULL AS STRING)          AS connecting_tables,
  CAST(NULL AS INT)             AS edge_table_count
FROM all_nodes

UNION ALL

SELECT
  'EDGES'                       AS result_type,
  CONCAT(producer_key, '->', consumer_key, ':', edge_kind) AS id,
  lower(edge_kind)              AS type,
  CONCAT(producer_key, ' -> ', consumer_key)               AS name,
  CASE WHEN edge_kind = 'TRIGGER'
       THEN 'Pipeline update triggered by job task'
       ELSE CONCAT('Tables: ', edge_tables) END             AS description,
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
  edge_kind                     AS label,
  CAST(NULL AS STRING)          AS job_id,
  CAST(NULL AS STRING)          AS pipeline_id,
  producer_key                  AS source_id,
  consumer_key                  AS target_id,
  edge_tables                   AS connecting_tables,
  edge_table_count              AS edge_table_count
FROM valid_edges
);
