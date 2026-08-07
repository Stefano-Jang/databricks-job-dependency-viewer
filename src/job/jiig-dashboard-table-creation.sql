-- =====================================================================
-- Impact table: one row per (failed entity, affected entity) pair
--
-- For every Job/Pipeline that failed inside the failure window, follow the
-- shared dependency edges downstream up to IMPACT_MAX_DEPTH hops and record
-- the shortest hop distance plus a sample path.
--
-- Edges come from the shared lineage layer (jiig-lineage-edges.sql) -- this
-- query no longer derives its own, so it can never disagree with the graph
-- table about what a dependency is.
--
-- Sources:
--   * the shared edge table                         -- dependency edges
--   * system.lakeflow.jobs / pipelines              -- SCD2, latest row
--   * system.lakeflow.job_run_timeline              -- job failures
--   * system.lakeflow.pipeline_update_timeline      -- pipeline update failures
-- =====================================================================
DECLARE OR REPLACE TARGET_WORKSPACE_ID   STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DASHBOARD_TBL         STRING DEFAULT {{dashboard_table}};
DECLARE OR REPLACE EDGE_TBL              STRING DEFAULT {{lineage_edge_table}};
DECLARE OR REPLACE FAILURE_LOOKBACK_DAYS INT    DEFAULT CAST({{failure_lookback_days}} AS INT);
DECLARE OR REPLACE IMPACT_MAX_DEPTH      INT    DEFAULT CAST({{impact_max_depth}} AS INT);

DECLARE OR REPLACE TARGET_SCHEMA STRING DEFAULT regexp_extract(DASHBOARD_TBL, '^(.*)\\.[^.]+$', 1);
CREATE SCHEMA IF NOT EXISTS identifier(TARGET_SCHEMA);

CREATE OR REPLACE TABLE identifier(DASHBOARD_TBL) AS (
WITH RECURSIVE time_bounds AS (
  SELECT timestampadd(DAY, -FAILURE_LOOKBACK_DAYS, current_timestamp()) AS failure_since_ts
),

-- ============================================
-- 1) Latest (SCD2) entity metadata
-- ============================================
-- The LakehouseMonitoringAnomalyDetection tag marks Databricks-managed
-- monitoring entities. Applied to jobs AND pipelines (pre-2.0 filtered only
-- jobs, so monitoring pipelines leaked into the impact list).
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

pipelines_latest AS (
  SELECT * FROM (
    SELECT
      pipeline_id, name, pipeline_type, created_by, run_as, tags,
      delete_time, change_time
    FROM system.lakeflow.pipelines
    WHERE workspace_id = TARGET_WORKSPACE_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1
  )
  WHERE COALESCE(NOT array_contains(map_keys(tags), 'LakehouseMonitoringAnomalyDetection'), true)
),

-- ============================================
-- 2) Failed entities within the window
-- ============================================
job_runs AS (
  SELECT
    jt.job_id,
    jt.run_id,
    MAX(COALESCE(jt.period_end_time, jt.period_start_time)) AS run_time,
    max_by(jt.result_state,
           CASE WHEN jt.result_state IS NOT NULL
                THEN COALESCE(jt.period_end_time, jt.period_start_time) END) AS result_state,
    max_by(jt.termination_code,
           CASE WHEN jt.termination_code IS NOT NULL
                THEN COALESCE(jt.period_end_time, jt.period_start_time) END) AS failure_detail
  FROM system.lakeflow.job_run_timeline jt
  CROSS JOIN time_bounds t
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.failure_since_ts
  GROUP BY jt.job_id, jt.run_id
),

job_failure_history AS (
  SELECT job_id, COUNT(*) AS failure_count
  FROM job_runs
  WHERE result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')
  GROUP BY job_id
),

latest_job_runs AS (
  SELECT * FROM job_runs WHERE result_state IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY run_time DESC, run_id DESC) = 1
),

failed_jobs AS (
  SELECT l.job_id, l.run_time AS last_failed_time, h.failure_count, l.failure_detail
  FROM latest_job_runs l
  JOIN job_failure_history h ON h.job_id = l.job_id
  WHERE l.result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')
),

pipeline_updates AS (
  SELECT
    put.pipeline_id,
    put.update_id,
    MAX(COALESCE(put.period_end_time, put.period_start_time)) AS update_time,
    max_by(put.result_state,
           CASE WHEN put.result_state IS NOT NULL
                THEN COALESCE(put.period_end_time, put.period_start_time) END) AS result_state,
    max_by(put.update_type, COALESCE(put.period_end_time, put.period_start_time)) AS update_type
  FROM system.lakeflow.pipeline_update_timeline put
  CROSS JOIN time_bounds t
  WHERE put.workspace_id = TARGET_WORKSPACE_ID
    AND put.update_type IN ('REFRESH', 'FULL_REFRESH')
    AND COALESCE(put.period_end_time, put.period_start_time) >= t.failure_since_ts
  GROUP BY put.pipeline_id, put.update_id
),

pipeline_failure_history AS (
  SELECT pipeline_id, COUNT(*) AS failure_count
  FROM pipeline_updates
  WHERE result_state = 'FAILED'
  GROUP BY pipeline_id
),

latest_pipeline_updates AS (
  SELECT * FROM pipeline_updates WHERE result_state IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY update_time DESC, update_id DESC) = 1
),

failed_pipelines AS (
  SELECT
    l.pipeline_id,
    l.update_time AS last_failed_time,
    h.failure_count,
    CONCAT('update_type=', l.update_type, ', update_id=', l.update_id) AS failure_detail
  FROM latest_pipeline_updates l
  JOIN pipeline_failure_history h ON h.pipeline_id = l.pipeline_id
  WHERE l.result_state = 'FAILED'
),

lineage_entity_owners AS (
  SELECT entity_kind, entity_key, max_by(owner_email, owner_email IS NOT NULL) AS owner_email
  FROM (
    SELECT producer_kind AS entity_kind, producer_key AS entity_key,
           producer_lineage_owner AS owner_email
    FROM identifier(EDGE_TBL) WHERE result_type = 'EDGE'
    UNION ALL
    SELECT consumer_kind, consumer_key, consumer_lineage_owner
    FROM identifier(EDGE_TBL) WHERE result_type = 'EDGE'
  )
  GROUP BY entity_kind, entity_key
),

failed_entities AS (
  SELECT
    'JOB'                                       AS entity_kind,
    fj.job_id                                   AS entity_key,
    COALESCE(
      CONCAT(j.name, CASE WHEN j.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END),
      CONCAT('Job ', fj.job_id, ' [UNREGISTERED]')
    )                                           AS entity_name,
    fj.last_failed_time,
    fj.failure_count,
    fj.failure_detail,
    COALESCE(j.creator_user_name, j.creator_id, o.owner_email) AS creator_email,
    COALESCE(j.run_as_user_name, j.run_as, o.owner_email)      AS run_as_email
  FROM failed_jobs fj
  LEFT JOIN jobs_latest j ON j.job_id = fj.job_id
  LEFT JOIN lineage_entity_owners o ON o.entity_kind = 'JOB' AND o.entity_key = fj.job_id

  UNION ALL

  SELECT
    'PIPELINE',
    fp.pipeline_id,
    COALESCE(
      CONCAT(p.name, CASE WHEN p.delete_time IS NOT NULL THEN ' [DELETED]' ELSE '' END),
      CONCAT('Pipeline ', fp.pipeline_id, ' [UNREGISTERED]')
    ),
    fp.last_failed_time,
    fp.failure_count,
    fp.failure_detail,
    COALESCE(p.created_by, o.owner_email),
    COALESCE(p.run_as, o.owner_email)
  FROM failed_pipelines fp
  LEFT JOIN pipelines_latest p ON p.pipeline_id = fp.pipeline_id
  LEFT JOIN lineage_entity_owners o ON o.entity_kind = 'PIPELINE' AND o.entity_key = fp.pipeline_id
),

-- ============================================
-- 3) Dependency edges (shared layer)
-- ============================================
all_edges AS (
  SELECT
    producer_kind, producer_key, consumer_kind, consumer_key,
    edge_kinds, edge_tables, consumer_lineage_owner
  FROM identifier(EDGE_TBL)
  WHERE result_type = 'EDGE'
),

-- Consumer display metadata. Entities present in lineage but missing from the
-- SCD2 tables (measured: most job producers on a large workspace, plus every
-- alert-backed job) fall back to the lineage owner rather than being dropped --
-- dropping them understated the blast radius badly.
consumer_meta AS (
  SELECT
    'JOB' AS entity_kind, job_id AS entity_key, name AS entity_name,
    description AS entity_description,
    COALESCE(creator_user_name, creator_id) AS creator_email,
    COALESCE(run_as_user_name, run_as)      AS run_as_email
  FROM jobs_latest WHERE delete_time IS NULL
  UNION ALL
  SELECT 'PIPELINE', pipeline_id, name, pipeline_type, created_by, run_as
  FROM pipelines_latest WHERE delete_time IS NULL
),

-- ============================================
-- 4) Multi-hop downstream traversal (cycle-safe, bounded depth)
-- ============================================
-- Recursion starts only from failed entities (a few hundred), not from every
-- node, which keeps it well inside the recursion row limit.
impact AS (
  SELECT
    f.entity_kind            AS failed_kind,
    f.entity_key             AS failed_key,
    f.entity_kind            AS affected_kind,
    f.entity_key             AS affected_key,
    0                        AS hop,
    CAST(array() AS ARRAY<STRING>) AS entering_tables,
    CAST(array() AS ARRAY<STRING>) AS entering_kinds,
    CAST(array() AS ARRAY<STRING>) AS path_tables,
    array(concat_ws(':', f.entity_kind, f.entity_key)) AS path_keys,
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
    e.edge_kinds,
    array_distinct(concat(i.path_tables, e.edge_tables)),
    array_append(i.path_keys, concat_ws(':', e.consumer_kind, e.consumer_key)),
    concat(
      i.path_names,
      CASE
        WHEN size(e.edge_tables) > 0 THEN array(concat_ws(', ', e.edge_tables))
        ELSE array(concat_ws('+', e.edge_kinds))
      END,
      array(COALESCE(cm.entity_name,
                     CONCAT(initcap(lower(e.consumer_kind)), ' ', e.consumer_key)))
    )
  FROM impact i
  JOIN all_edges e
    ON e.producer_kind = i.affected_kind AND e.producer_key = i.affected_key
  LEFT JOIN consumer_meta cm
    ON cm.entity_kind = e.consumer_kind AND cm.entity_key = e.consumer_key
  WHERE i.hop < IMPACT_MAX_DEPTH
    AND NOT array_contains(i.path_keys, concat_ws(':', e.consumer_kind, e.consumer_key))
),

impact_min AS (
  SELECT failed_kind, failed_key, affected_kind, affected_key, MIN(hop) AS hop_distance
  FROM impact
  WHERE hop >= 1
  GROUP BY 1, 2, 3, 4
),

-- Aggregate the connecting tables at the shortest distance and keep one
-- representative path. min() over the joined string is deterministic (same
-- input always yields the same path) which matters for a table that is
-- rebuilt every 30 minutes and diffed by humans.
impact_shortest AS (
  SELECT
    i.failed_kind, i.failed_key, i.affected_kind, i.affected_key,
    m.hop_distance,
    concat_ws(', ', array_sort(array_distinct(flatten(collect_list(i.path_tables)))))     AS affected_tables,
    array_sort(array_distinct(flatten(collect_list(i.entering_kinds))))                   AS affected_via,
    MIN(concat_ws(' -> ', i.path_names)) AS impact_path
  FROM impact i
  JOIN impact_min m
    ON  m.failed_kind   = i.failed_kind   AND m.failed_key   = i.failed_key
    AND m.affected_kind = i.affected_kind AND m.affected_key = i.affected_key
    AND m.hop_distance  = i.hop
  GROUP BY 1, 2, 3, 4, 5
),

-- Owner for every affected entity, whatever its kind. Leaf consumers
-- (dashboards, Genie spaces, queries, alerts) have no system table, so their
-- owner comes from table_lineage.created_by -- verified populated.
affected_meta AS (
  SELECT entity_kind, entity_key, entity_name, entity_description,
         creator_email, run_as_email
  FROM consumer_meta
  UNION ALL
  SELECT consumer_kind, consumer_key,
         CONCAT(initcap(lower(consumer_kind)), ' ', substr(consumer_key, 1, 8)),
         lower(consumer_kind),
         MAX(consumer_lineage_owner), MAX(consumer_lineage_owner)
  FROM all_edges
  WHERE consumer_kind NOT IN ('JOB', 'PIPELINE')
  GROUP BY consumer_kind, consumer_key
)

-- ============================================
-- 5) Final shape
-- ============================================
-- LEFT JOIN: a failed entity with no downstream consumer still gets one row
-- (NULL affected_*) so the failed list itself stays complete.
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
  COALESCE(am.entity_name,
           CONCAT(initcap(lower(s.affected_kind)), ' ', s.affected_key)) AS affected_name,
  s.affected_tables       AS affected_tables,
  am.creator_email        AS affected_creator_email,
  am.run_as_email         AS affected_run_as_email,
  am.entity_description   AS affected_descriptions,
  s.hop_distance          AS hop_distance,
  s.impact_path           AS impact_path,
  concat_ws('+', s.affected_via) AS affected_via
FROM failed_entities f
LEFT JOIN impact_shortest s
  ON s.failed_kind = f.entity_kind AND s.failed_key = f.entity_key
LEFT JOIN affected_meta am
  ON am.entity_kind = s.affected_kind AND am.entity_key = s.affected_key
ORDER BY f.last_failed_time DESC, failed_id, hop_distance, affected_id
);
