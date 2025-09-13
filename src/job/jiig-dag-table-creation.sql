-- =========================
-- Create and Refresh DAG Relationships Table (Aligned with Dashboard Logic)
-- =========================
DECLARE OR REPLACE UID_MAP_TBL STRING DEFAULT {{user_id_map_table_name}};
DECLARE OR REPLACE LDP_ERR_TBL STRING DEFAULT {{ldp_error_table_full_name}};
DECLARE OR REPLACE TARGET_WORKSPACE_ID STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DAG_TBL STRING DEFAULT {{jiig_dag_table}};

-- Activity inclusion window (default: last 24 hours)
DECLARE OR REPLACE RECENT_WINDOW_HOURS INTERVAL DAY DEFAULT INTERVAL 1 DAY;

CREATE TABLE IF NOT EXISTS identifier(LDP_ERR_TBL)(
  pipeline_name STRING,
  pipeline_id STRING,
  event_type STRING,
  error_count BIGINT,
  affected_runs BIGINT,
  first_error TIMESTAMP,
  last_error TIMESTAMP,
  sample_error_messages ARRAY<STRING>,
  sample_message STRING
);

CREATE OR REPLACE TABLE identifier(DAG_TBL) AS (
-- ============================================
-- 0) Time bounds
-- ============================================
WITH time_bounds AS (
  SELECT
    CURRENT_TIMESTAMP()                           AS now_ts,
    CURRENT_TIMESTAMP() - RECENT_WINDOW_HOURS     AS since_ts
),

-- ============================================
-- 1) Failed job runs (dashboard-aligned)
--     * Added time window filter on failed_at
-- ============================================
fail_runs_job AS (
  SELECT DISTINCT
    jt.job_id,
    jt.run_id,
    COALESCE(jt.period_end_time, jt.period_start_time) AS failed_at
  FROM system.lakeflow.job_run_timeline jt
  JOIN time_bounds t ON 1=1
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.result_state IN ('FAILED','ERROR','TIMED_OUT')
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.since_ts   -- <-- time filter
),

failed_jobs_last AS (
  SELECT
    job_id,
    MAX(failed_at) AS last_failed_time,
    MIN(failed_at) AS first_failed_time,
    COUNT(*)       AS failure_count
  FROM fail_runs_job
  GROUP BY job_id
),

-- ============================================
-- 2) Failed pipelines (dashboard-aligned via LDP_ERR_TBL)
--     * Added time window filter on last_error
-- ============================================
failed_pipelines_last AS (
  SELECT
    pipeline_id,
    MAX(last_error) AS last_failed_time,
    MIN(last_error) AS first_failed_time,
    COUNT(*)        AS failure_count
  FROM identifier(LDP_ERR_TBL)
  JOIN time_bounds t ON 1=1
  WHERE last_error >= t.since_ts                                          -- <-- time filter
  GROUP BY pipeline_id
),

-- ============================================
-- 3) Failed entity master (JOB + PIPELINE)
-- ============================================
failed_entity_master AS (
  -- JOB
  SELECT
    'JOB'        AS failed_type,
    j.job_id     AS failed_id,
    j.name       AS failed_name,
    j.description AS failed_description,
    j.creator_id AS failed_creator_id,
    j.run_as     AS failed_run_as_id,
    NULL         AS failed_creator_email,
    NULL         AS failed_run_as_email,
    fj.last_failed_time,
    fj.first_failed_time,
    fj.failure_count
  FROM system.lakeflow.jobs j
  JOIN failed_jobs_last fj ON j.job_id = fj.job_id
  WHERE j.workspace_id = TARGET_WORKSPACE_ID
    AND array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false

  UNION ALL
  -- PIPELINE
  SELECT
    'PIPELINE'    AS failed_type,
    p.pipeline_id AS failed_id,
    p.name        AS failed_name,
    p.pipeline_type AS failed_description,
    NULL          AS failed_creator_id,
    NULL          AS failed_run_as_id,
    p.created_by  AS failed_creator_email,
    p.run_as      AS failed_run_as_email,
    fp.last_failed_time,
    fp.first_failed_time,
    fp.failure_count
  FROM system.lakeflow.pipelines p
  JOIN failed_pipelines_last fp ON p.pipeline_id = fp.pipeline_id
  WHERE p.workspace_id = TARGET_WORKSPACE_ID
),

-- ============================================
-- 4) Recent activity sets (24h window)
--     (unchanged)
-- ============================================
recent_job_activity AS (
  SELECT DISTINCT jt.job_id
  FROM system.lakeflow.job_run_timeline jt
  JOIN time_bounds t ON 1=1
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.since_ts
),

job_last_activity AS (
  SELECT
    jt.job_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_activity_time
  FROM system.lakeflow.job_run_timeline jt
  JOIN time_bounds t ON 1=1
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.since_ts
  GROUP BY jt.job_id
),

lineage AS (
  SELECT
    l.event_time,
    l.entity_type,
    l.entity_id,
    l.source_table_full_name,
    l.target_table_full_name,
    l.source_type,
    l.target_type,
    l.entity_metadata.job_info.job_id,
    l.entity_metadata.job_info.job_run_id,
    l.entity_metadata.dlt_pipeline_info.dlt_pipeline_id,
    l.entity_metadata.dlt_pipeline_info.dlt_update_id
  FROM system.access.table_lineage l
  WHERE l.workspace_id = TARGET_WORKSPACE_ID
    AND l.entity_type IN ('JOB','PIPELINE')
),

recent_pipeline_activity AS (
  SELECT DISTINCT
    l.dlt_pipeline_id AS pipeline_id
  FROM lineage l
  JOIN time_bounds t ON 1=1
  WHERE l.entity_type = 'PIPELINE'
    AND l.event_time >= t.since_ts
),

pipeline_last_activity AS (
  SELECT
    l.dlt_pipeline_id AS pipeline_id,
    MAX(l.event_time) AS last_activity_time
  FROM lineage l
  JOIN time_bounds t ON 1=1
  WHERE l.entity_type = 'PIPELINE'
    AND l.event_time >= t.since_ts
  GROUP BY l.dlt_pipeline_id
),

-- ============================================
-- 5) Active entity sets (unchanged)
-- ============================================
active_jobs AS (
  SELECT DISTINCT j.job_id AS entity_id
  FROM system.lakeflow.jobs j
  WHERE j.workspace_id = TARGET_WORKSPACE_ID
    AND array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false
    AND (
      j.job_id IN (SELECT job_id FROM failed_jobs_last)
      OR j.job_id IN (SELECT job_id FROM recent_job_activity)
    )
),
active_pipelines AS (
  SELECT DISTINCT p.pipeline_id AS entity_id
  FROM system.lakeflow.pipelines p
  WHERE p.workspace_id = TARGET_WORKSPACE_ID
    AND (
      p.pipeline_id IN (SELECT pipeline_id FROM failed_pipelines_last)
      OR p.pipeline_id IN (SELECT pipeline_id FROM recent_pipeline_activity)
    )
),

-- ============================================
-- 6~10) (unchanged) meta join, edges, final select
-- ============================================
all_entity_master AS (
  SELECT
    'JOB' AS entity_type,
    j.job_id AS entity_id,
    j.name AS entity_name,
    j.description AS entity_description,
    j.creator_id AS entity_creator_id,
    j.run_as AS entity_run_as_id,
    NULL AS entity_creator_email,
    NULL AS entity_run_as_email,
    j.change_time AS entity_created_time
  FROM system.lakeflow.jobs j
  WHERE j.workspace_id = TARGET_WORKSPACE_ID
    AND array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false
    AND j.job_id IN (SELECT entity_id FROM active_jobs)

  UNION ALL
  SELECT
    'PIPELINE',
    p.pipeline_id,
    p.name,
    p.pipeline_type,
    NULL,
    NULL,
    p.created_by,
    p.run_as,
    p.change_time
  FROM system.lakeflow.pipelines p
  WHERE p.workspace_id = TARGET_WORKSPACE_ID
    AND p.pipeline_id IN (SELECT entity_id FROM active_pipelines)
),

entity_target_tables AS (
  SELECT DISTINCT
    aem.entity_type, aem.entity_id, aem.entity_name, l.target_table_full_name
  FROM all_entity_master aem
  JOIN lineage l
    ON (
         (aem.entity_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id          = aem.entity_id)
      OR (aem.entity_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id = aem.entity_id)
       )
  WHERE l.target_table_full_name IS NOT NULL
),
entity_source_tables AS (
  SELECT DISTINCT
    aem.entity_type, aem.entity_id, aem.entity_name, l.source_table_full_name
  FROM all_entity_master aem
  JOIN lineage l
    ON (
         (aem.entity_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id          = aem.entity_id)
      OR (aem.entity_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id = aem.entity_id)
       )
  WHERE l.source_table_full_name IS NOT NULL
),

entity_relationships AS (
  SELECT DISTINCT
    ett.entity_type AS producer_type,
    ett.entity_id   AS producer_id,
    ett.entity_name AS producer_name,
    est.entity_type AS consumer_type,
    est.entity_id   AS consumer_id,
    est.entity_name AS consumer_name,
    ett.target_table_full_name AS connecting_table
  FROM entity_target_tables ett
  JOIN entity_source_tables est
    ON ett.target_table_full_name = est.source_table_full_name
  WHERE NOT (ett.entity_type = est.entity_type AND ett.entity_id = est.entity_id)
),

entity_master_complete AS (
  SELECT
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    aem.entity_description,
    COALESCE(idmap_creator.email, aem.entity_creator_email) AS entity_creator_email,
    COALESCE(idmap_runas.email,   aem.entity_run_as_email)  AS entity_run_as_email,
    CASE
      WHEN aem.entity_type = 'JOB'      AND fj.job_id      IS NOT NULL THEN true
      WHEN aem.entity_type = 'PIPELINE' AND fp.pipeline_id IS NOT NULL THEN true
      ELSE false
    END AS is_failed,
    COALESCE(fj.last_failed_time,  fp.last_failed_time)  AS last_failed_time,
    COALESCE(fj.first_failed_time, fp.first_failed_time) AS first_failed_time,
    COALESCE(fj.failure_count,     fp.failure_count, 0)  AS failure_count,
    COALESCE(ja.last_activity_time, pa.last_activity_time) AS last_activity_time,
    aem.entity_created_time AS entity_created_time,
    CASE WHEN aem.entity_type='JOB'      THEN aem.entity_id END AS original_job_id,
    CASE WHEN aem.entity_type='PIPELINE' THEN aem.entity_id END AS original_pipeline_id
  FROM all_entity_master aem
  LEFT JOIN identifier(UID_MAP_TBL) idmap_creator
    ON aem.entity_creator_id = idmap_creator.id
  LEFT JOIN identifier(UID_MAP_TBL) idmap_runas
    ON aem.entity_run_as_id  = idmap_runas.id
  LEFT JOIN failed_jobs_last fj
    ON aem.entity_type='JOB' AND aem.entity_id=fj.job_id
  LEFT JOIN failed_pipelines_last fp
    ON aem.entity_type='PIPELINE' AND aem.entity_id=fp.pipeline_id
  LEFT JOIN job_last_activity ja
    ON aem.entity_type='JOB' AND aem.entity_id = ja.job_id
  LEFT JOIN pipeline_last_activity pa
    ON aem.entity_type='PIPELINE' AND aem.entity_id = pa.pipeline_id
),

dag_nodes AS (
  SELECT
    a.entity_id                        AS node_id,
    lower(a.entity_type)               AS node_type,
    a.entity_name                      AS node_name,
    a.entity_description               AS node_description,
    a.entity_creator_email             AS node_creator_email,
    a.entity_run_as_email              AS node_run_as_email,
    a.is_failed                        AS node_is_failed,
    a.last_failed_time                 AS node_last_failed_time,
    a.first_failed_time                AS node_first_failed_time,
    a.failure_count                    AS node_failure_count,
    a.last_activity_time               AS node_last_activity_time,
    a.entity_created_time              AS node_created_time,
    a.original_job_id                  AS node_job_id,
    a.original_pipeline_id             AS node_pipeline_id,
    CASE WHEN a.is_failed THEN 'FAILED' ELSE 'HEALTHY' END AS node_status,
    upper(a.entity_type)               AS node_label
  FROM entity_master_complete a
),
dag_edges AS (
  SELECT
    CONCAT(er.producer_id, '->', er.consumer_id) AS edge_id,
    er.producer_id      AS source_node_id,
    lower(er.producer_type) AS source_node_type,
    er.producer_name    AS source_node_name,
    er.consumer_id      AS target_node_id,
    lower(er.consumer_type) AS target_node_type,
    er.consumer_name    AS target_node_name,
    er.connecting_table AS edge_table,
    'DEPENDENCY'        AS edge_label
  FROM entity_relationships er
)

SELECT 
  'NODES' AS result_type,
  node_id AS id,
  node_type AS type,
  node_name AS name,
  node_description AS description,
  node_creator_email AS creator_email,
  node_run_as_email AS run_as_email,
  node_is_failed AS is_failed,
  node_last_failed_time AS last_failed_time,
  node_first_failed_time AS first_failed_time,
  node_failure_count AS failure_count,
  node_last_activity_time AS last_activity_time,
  node_created_time AS created_time,
  node_status AS status,
  node_label AS label,
  node_job_id AS job_id,
  node_pipeline_id AS pipeline_id,
  NULL AS source_id,
  NULL AS target_id,
  NULL AS connecting_table
FROM dag_nodes

UNION ALL

SELECT 
  'EDGES' AS result_type,
  edge_id AS id,
  'dependency' AS type,
  CONCAT(source_node_name, ' -> ', target_node_name) AS name,
  CONCAT('Table: ', edge_table) AS description,
  NULL AS creator_email,
  NULL AS run_as_email,
  NULL AS is_failed,
  NULL AS last_failed_time,
  NULL AS first_failed_time,
  NULL AS failure_count,
  NULL AS last_activity_time,
  NULL AS created_time,
  'ACTIVE' AS status,
  edge_label AS label,
  NULL AS job_id,
  NULL AS pipeline_id,
  source_node_id AS source_id,
  target_node_id AS target_id,
  edge_table AS connecting_table
FROM dag_edges

ORDER BY result_type, id
);
