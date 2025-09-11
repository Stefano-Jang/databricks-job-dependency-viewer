-- =========================
-- Create and Refresh DAG Relationships Table (Aligned with Dashboard Logic)
-- =========================
DECLARE OR REPLACE UID_MAP_TBL STRING DEFAULT {{user_id_map_table_name}};
DECLARE OR REPLACE LDP_ERR_TBL STRING DEFAULT {{ldp_error_table_full_name}};
DECLARE OR REPLACE TARGET_WORKSPACE_ID STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DAG_TBL STRING DEFAULT {{jiig_dag_table}};

-- 최근 Healthy 노드 포함 범위 (기본: 24시간)
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
-- 0) 공통 시간 경계
-- ============================================
WITH time_bounds AS (
  SELECT
    CURRENT_TIMESTAMP()                           AS now_ts,
    CURRENT_TIMESTAMP() - RECENT_WINDOW_HOURS     AS since_ts
),

-- ============================================
-- 1) 실패 런 수집 (JOB) – Dashboard 기준 정합
-- ============================================
fail_runs_job AS (
  SELECT DISTINCT
    job_id,
    run_id,
    COALESCE(period_end_time, period_start_time) AS failed_at
  FROM system.lakeflow.job_run_timeline jt
  JOIN time_bounds t
    ON 1=1
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.result_state IN ('FAILED','ERROR','TIMED_OUT')
    AND jt.run_type = 'JOB_RUN'
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
-- 2) 실패 런 수집 (PIPELINE) – Dashboard 기준 정합
--    LDP_ERR_TBL.last_error 사용
-- ============================================
failed_pipelines_last AS (
  SELECT
    pipeline_id,
    MAX(last_error) AS last_failed_time,
    MIN(last_error) AS first_failed_time,
    COUNT(*)        AS failure_count
  FROM identifier(LDP_ERR_TBL)
  GROUP BY pipeline_id
),

-- ============================================
-- 3) 실패 엔터티 마스터 (JOB + PIPELINE) – Dashboard와 동일 철학
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
  WHERE array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false

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
),

-- ============================================
-- 4) 최근 활동 엔터티 수집 (Healthy 포함 범위 축소)
--    - JOB: 최근 24h 내 period_end_time 존재
--    - PIPELINE: 최근 24h 내 lineage 이벤트 또는
--                최근 24h 내 활동 JOB/실패 엔터티와의 연결로 유입
-- ============================================
recent_job_activity AS (
  SELECT DISTINCT
    jt.job_id
  FROM system.lakeflow.job_run_timeline jt
  JOIN time_bounds t ON 1=1
  WHERE jt.workspace_id = TARGET_WORKSPACE_ID
    AND jt.run_type = 'JOB_RUN'
    AND COALESCE(jt.period_end_time, jt.period_start_time) >= t.since_ts
),

lineage AS (
  SELECT
    event_time,
    entity_type,                         -- 'JOB' or 'PIPELINE'
    entity_id,
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type,
    entity_metadata.job_info.job_id                   AS job_id,
    entity_metadata.job_info.job_run_id               AS job_run_id,
    entity_metadata.dlt_pipeline_info.dlt_pipeline_id AS dlt_pipeline_id,
    entity_metadata.dlt_pipeline_info.dlt_update_id   AS dlt_update_id
  FROM system.access.table_lineage
  WHERE workspace_id = TARGET_WORKSPACE_ID
    AND entity_type IN ('JOB','PIPELINE')
),

recent_pipeline_activity AS (
  SELECT DISTINCT
    l.dlt_pipeline_id AS pipeline_id
  FROM lineage l
  JOIN time_bounds t ON 1=1
  WHERE l.entity_type = 'PIPELINE'
    AND l.event_time >= t.since_ts
),

-- ============================================
-- 5) 전체 “활성(Active)” 엔터티 집합
--    - 실패 엔터티(대시보드 기준)
--    - 또는 최근 활동 JOB / 최근 활동 PIPELINE
-- ============================================
active_jobs AS (
  SELECT DISTINCT j.job_id AS entity_id
  FROM system.lakeflow.jobs j
  WHERE array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false
    AND (j.job_id IN (SELECT job_id FROM failed_jobs_last)
         OR j.job_id IN (SELECT job_id FROM recent_job_activity))
),
active_pipelines AS (
  SELECT DISTINCT p.pipeline_id AS entity_id
  FROM system.lakeflow.pipelines p
  WHERE p.pipeline_id IN (SELECT pipeline_id FROM failed_pipelines_last)
     OR p.pipeline_id IN (SELECT pipeline_id FROM recent_pipeline_activity)
),
all_entity_master AS (
  -- JOB 메타
  SELECT
    'JOB'                         AS entity_type,
    j.job_id                      AS entity_id,
    j.name                        AS entity_name,
    j.description                 AS entity_description,
    j.creator_id                  AS entity_creator_id,
    j.run_as                      AS entity_run_as_id,
    NULL                          AS entity_creator_email,
    NULL                          AS entity_run_as_email,
    j.change_time                 AS entity_created_time
  FROM system.lakeflow.jobs j
  WHERE array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false
    AND j.job_id IN (SELECT entity_id FROM active_jobs)

  UNION ALL
  -- PIPELINE 메타
  SELECT
    'PIPELINE'                    AS entity_type,
    p.pipeline_id                 AS entity_id,
    p.name                        AS entity_name,
    p.pipeline_type               AS entity_description,
    NULL                          AS entity_creator_id,
    NULL                          AS entity_run_as_id,
    p.created_by                  AS entity_creator_email,
    p.run_as                      AS entity_run_as_email,
    p.change_time                 AS entity_created_time
  FROM system.lakeflow.pipelines p
  WHERE p.pipeline_id IN (SELECT entity_id FROM active_pipelines)
),

-- ============================================
-- 6) 엔터티가 쓰는(생산) 타깃 테이블 / 읽는(소비) 소스 테이블
--    * 엣지는 target -> source 일치로 구성 (자기참조 제외)
-- ============================================
entity_target_tables AS (
  SELECT DISTINCT
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    l.target_table_full_name
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
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    l.source_table_full_name
  FROM all_entity_master aem
  JOIN lineage l
    ON (
         (aem.entity_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id          = aem.entity_id)
      OR (aem.entity_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id = aem.entity_id)
       )
  WHERE l.source_table_full_name IS NOT NULL
),

-- ============================================
-- 7) 엔터티 간 관계(엣지) – 다양성/정확성 강화
--    동일 테이블을 매개로 생산자→소비자 연결, 자기참조 제거
-- ============================================
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

-- ============================================
-- 8) 실패 상태/이메일 매핑 결합
-- ============================================
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
),

-- ============================================
-- 9) DAG 노드/엣지 (출력 스키마 유지)
-- ============================================
dag_nodes AS (
  SELECT
    entity_id                        AS node_id,
    lower(entity_type)               AS node_type,
    entity_name                      AS node_name,
    entity_description               AS node_description,
    entity_creator_email             AS node_creator_email,
    entity_run_as_email              AS node_run_as_email,
    is_failed                        AS node_is_failed,
    last_failed_time                 AS node_last_failed_time,
    first_failed_time                AS node_first_failed_time,
    failure_count                    AS node_failure_count,
    entity_created_time              AS node_created_time,
    original_job_id                  AS node_job_id,
    original_pipeline_id             AS node_pipeline_id,
    CASE WHEN is_failed THEN 'FAILED' ELSE 'HEALTHY' END AS node_status,
    upper(entity_type)               AS node_label
  FROM entity_master_complete
),
dag_edges AS (
  SELECT
    CONCAT(producer_id, '->', consumer_id) AS edge_id,
    producer_id      AS source_node_id,
    lower(producer_type) AS source_node_type,
    producer_name    AS source_node_name,
    consumer_id      AS target_node_id,
    lower(consumer_type) AS target_node_type,
    consumer_name    AS target_node_name,
    connecting_table AS edge_table,
    'DEPENDENCY'     AS edge_label
  FROM entity_relationships
)

-- ============================================
-- 10) 최종 출력 (스키마/순서 고정)
-- ============================================
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
