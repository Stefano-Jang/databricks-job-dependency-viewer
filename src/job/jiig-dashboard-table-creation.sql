-- =====================================================================
-- Create and Refresh Dashboard Table (multi-hop failure impact analysis)
--
-- For every Job/Pipeline that failed inside the failure window, follow the
-- dependency edges (table lineage + job->pipeline triggers) downstream up to
-- IMPACT_MAX_DEPTH hops and emit one row per (failed entity, affected entity)
-- with the shortest hop distance and a sample impact path.
--
-- Sources (all native system tables, no auxiliary jobs required):
--   * system.lakeflow.jobs / pipelines              -- SCD2, latest row used
--   * system.lakeflow.job_run_timeline              -- job failures
--   * system.lakeflow.pipeline_update_timeline      -- pipeline (SDP) update
--                                                      failures & triggers
--   * system.access.table_lineage                   -- table-level edges
-- =====================================================================
DECLARE OR REPLACE TARGET_WORKSPACE_ID   STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DASHBOARD_TBL         STRING DEFAULT {{dashboard_table}};
DECLARE OR REPLACE FAILURE_LOOKBACK_DAYS INT    DEFAULT CAST({{failure_lookback_days}} AS INT);
DECLARE OR REPLACE LINEAGE_LOOKBACK_DAYS INT    DEFAULT CAST({{lineage_lookback_days}} AS INT);
DECLARE OR REPLACE IMPACT_MAX_DEPTH      INT    DEFAULT CAST({{impact_max_depth}} AS INT);

-- Bootstrap the target schema (catalog.schema part of the table name)
DECLARE OR REPLACE TARGET_SCHEMA STRING DEFAULT regexp_extract(DASHBOARD_TBL, '^(.*)\\.[^.]+$', 1);
CREATE SCHEMA IF NOT EXISTS identifier(TARGET_SCHEMA);

CREATE OR REPLACE TABLE identifier(DASHBOARD_TBL) AS (
WITH RECURSIVE time_bounds AS (
  SELECT
    timestampadd(DAY, -FAILURE_LOOKBACK_DAYS, current_timestamp()) AS failure_since_ts,
    timestampadd(DAY, -LINEAGE_LOOKBACK_DAYS, current_timestamp()) AS lineage_since_ts,
    date_sub(current_date(), LINEAGE_LOOKBACK_DAYS)                AS lineage_since_date
),

-- ============================================
-- 1) Latest (SCD2) entity metadata
-- ============================================
-- Latest row regardless of deletion: failed entities stay visible even when
-- they were deleted after the failure (forensics); consumers must be alive.
jobs_latest AS (
  SELECT * FROM (
    SELECT
      job_id, name, description, creator_id, creator_user_name,
      run_as, run_as_user_name, tags, delete_time, change_time
    FROM system.lakeflow.jobs
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
  )
  WHERE COALESCE(NOT array_contains(map_keys(tags), 'LakehouseMonitoringAnomalyDetection'), true)
),

jobs_alive AS (
  SELECT * FROM jobs_latest WHERE delete_time IS NULL
),

pipelines_latest AS (
  SELECT * FROM (
    SELECT
      pipeline_id, name, pipeline_type, created_by, run_as, delete_time, change_time
    FROM system.lakeflow.pipelines
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
  )
),

pipelines_alive AS (
  SELECT * FROM pipelines_latest WHERE delete_time IS NULL
),

-- ============================================
-- 2) Failed entities within the window
-- ============================================
failed_jobs AS (
  SELECT
    jt.job_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS last_failed_time,
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

failed_entities AS (
  SELECT
    'JOB'                                       AS entity_kind,
    j.job_id                                    AS entity_key,
    CONCAT(j.name, CASE WHEN j.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END) AS entity_name,
    fj.last_failed_time,
    fj.failure_count,
    fj.failure_detail,
    COALESCE(j.creator_user_name, j.creator_id) AS creator_email,
    COALESCE(j.run_as_user_name, j.run_as)      AS run_as_email
  FROM failed_jobs fj
  JOIN jobs_latest j ON j.job_id = fj.job_id

  UNION ALL

  SELECT
    'PIPELINE',
    p.pipeline_id,
    CONCAT(p.name, CASE WHEN p.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END),
    fp.last_failed_time,
    fp.failure_count,
    fp.failure_detail,
    p.created_by,
    p.run_as
  FROM failed_pipelines fp
  JOIN pipelines_latest p ON p.pipeline_id = fp.pipeline_id
),

-- ============================================
-- 3) Dependency edges (lineage + job->pipeline triggers)
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

table_edges AS (
  SELECT
    w.entity_kind AS producer_kind,
    w.entity_key  AS producer_key,
    r.entity_kind AS consumer_kind,
    r.entity_key  AS consumer_key,
    sort_array(collect_set(w.table_full_name)) AS edge_tables
  FROM entity_writes w
  JOIN entity_reads r
    ON w.table_full_name = r.table_full_name
  WHERE NOT (w.entity_kind = r.entity_kind AND w.entity_key = r.entity_key)
  GROUP BY 1, 2, 3, 4
),

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

-- Edges enriched with consumer metadata; consumers that no longer exist
-- (deleted entities) are dropped by the inner joins. One row per
-- (producer, consumer) pair — parallel table/trigger edges are merged so the
-- recursive traversal does not multiply paths.
all_edges AS (
  SELECT
    e.producer_kind, e.producer_key,
    e.consumer_kind, e.consumer_key,
    array_distinct(flatten(collect_list(e.edge_tables))) AS edge_tables,
    c.consumer_name, c.consumer_description, c.consumer_creator_email, c.consumer_run_as_email
  FROM (
    SELECT producer_kind, producer_key, consumer_kind, consumer_key, edge_tables FROM table_edges
    UNION ALL
    SELECT producer_kind, producer_key, consumer_kind, consumer_key,
           CAST(array() AS ARRAY<STRING>) FROM trigger_edges
  ) e
  JOIN (
    SELECT
      'JOB' AS consumer_kind, job_id AS consumer_key, name AS consumer_name,
      description AS consumer_description,
      COALESCE(creator_user_name, creator_id) AS consumer_creator_email,
      COALESCE(run_as_user_name, run_as)      AS consumer_run_as_email
    FROM jobs_alive
    UNION ALL
    SELECT 'PIPELINE', pipeline_id, name, pipeline_type, created_by, run_as
    FROM pipelines_alive
  ) c
    ON c.consumer_kind = e.consumer_kind AND c.consumer_key = e.consumer_key
  GROUP BY e.producer_kind, e.producer_key, e.consumer_kind, e.consumer_key,
           c.consumer_name, c.consumer_description, c.consumer_creator_email, c.consumer_run_as_email
),

-- ============================================
-- 4) Multi-hop downstream traversal (cycle-safe, bounded depth)
-- ============================================
impact AS (
  -- hop 0: each failed entity is the root of its own impact tree
  SELECT
    f.entity_kind            AS failed_kind,
    f.entity_key             AS failed_key,
    f.entity_kind            AS affected_kind,
    f.entity_key             AS affected_key,
    0                        AS hop,
    CAST(array() AS ARRAY<STRING>) AS entering_tables,
    array(f.entity_key)      AS path_keys,
    array(f.entity_name)     AS path_names
  FROM failed_entities f

  UNION ALL

  SELECT
    i.failed_kind,
    i.failed_key,
    e.consumer_kind,
    e.consumer_key,
    i.hop + 1,
    e.edge_tables,
    array_append(i.path_keys, e.consumer_key),
    array_append(i.path_names, e.consumer_name)
  FROM impact i
  JOIN all_edges e
    ON e.producer_kind = i.affected_kind AND e.producer_key = i.affected_key
  WHERE i.hop < IMPACT_MAX_DEPTH
    AND NOT array_contains(i.path_keys, e.consumer_key)
),

-- Shortest hop per (failed, affected) pair; aggregate the tables and keep
-- one sample path at that distance.
impact_min AS (
  SELECT failed_kind, failed_key, affected_kind, affected_key, MIN(hop) AS hop_distance
  FROM impact
  WHERE hop >= 1
  GROUP BY 1, 2, 3, 4
),

impact_shortest AS (
  SELECT
    i.failed_kind, i.failed_key, i.affected_kind, i.affected_key,
    m.hop_distance,
    concat_ws(', ', sort_array(array_distinct(flatten(collect_list(i.entering_tables))))) AS affected_tables,
    min(concat_ws(' -> ', i.path_names)) AS impact_path
  FROM impact i
  JOIN impact_min m
    ON  m.failed_kind  = i.failed_kind  AND m.failed_key  = i.failed_key
    AND m.affected_kind = i.affected_kind AND m.affected_key = i.affected_key
    AND m.hop_distance = i.hop
  GROUP BY 1, 2, 3, 4, 5
),

-- ============================================
-- 5) Final shape (legacy dashboard columns + multi-hop additions)
-- ============================================
affected_meta AS (
  SELECT
    'JOB' AS entity_kind, job_id AS entity_key, name AS entity_name,
    description AS entity_description,
    COALESCE(creator_user_name, creator_id) AS creator_email,
    COALESCE(run_as_user_name, run_as)      AS run_as_email
  FROM jobs_alive
  UNION ALL
  SELECT 'PIPELINE', pipeline_id, name, pipeline_type, created_by, run_as
  FROM pipelines_alive
)

-- LEFT JOINs: a failed entity with no downstream consumers still gets one
-- row (NULL affected_*) so the failed list itself is complete.
SELECT
  f.entity_key            AS failed_id,
  lower(f.entity_kind)    AS failed_type,
  f.entity_name           AS failed_name,
  f.last_failed_time      AS last_failed_time,
  f.failure_count         AS failure_count,
  f.failure_detail        AS failure_detail,
  f.creator_email         AS failed_creator_email,
  f.run_as_email          AS failed_run_as_email,
  s.affected_key          AS affected_id,
  lower(s.affected_kind)  AS affected_type,
  am.entity_name          AS affected_name,
  s.affected_tables       AS affected_tables,
  am.creator_email        AS affected_creator_email,
  am.run_as_email         AS affected_run_as_email,
  am.entity_description   AS affected_descriptions,
  s.hop_distance          AS hop_distance,
  s.impact_path           AS impact_path
FROM failed_entities f
LEFT JOIN impact_shortest s
  ON s.failed_kind = f.entity_kind AND s.failed_key = f.entity_key
LEFT JOIN affected_meta am
  ON am.entity_kind = s.affected_kind AND am.entity_key = s.affected_key
ORDER BY f.last_failed_time DESC, failed_id, hop_distance, affected_id
);
