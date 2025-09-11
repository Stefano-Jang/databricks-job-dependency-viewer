-- =========================
-- Create and Refresh DAG Relationships Table
-- Purpose: Set up the physical table for the DAG visualizer
-- =========================
DECLARE OR REPLACE UID_MAP_TBL STRING DEFAULT {{user_id_map_table_name}};
DECLARE OR REPLACE LDP_ERR_TBL STRING DEFAULT {{ldp_error_table_full_name}};
DECLARE OR REPLACE TARGET_WORKSPACE_ID STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DAG_TBL STRING DEFAULT {{jiig_dag_table}};
DECLARE OR REPLACE REFERENCE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE TIME_WINDOW_HOURS INTERVAL DAY DEFAULT INTERVAL 1 DAY;

CREATE table if not exists identifier(LDP_ERR_TBL)(
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

-- Step 1: Create the table (run once)
CREATE OR REPLACE TABLE identifier(DAG_TBL) AS (
-- =========================
-- DAG Relationships: Jobs and Pipelines Graph for st_link_analysis
-- Purpose: Create comprehensive relationship graph for all jobs/pipelines to support DAG visualization
--          Compatible with Streamlit st_link_analysis component
-- =========================
--
-- USAGE INSTRUCTIONS:
-- 1. Create Physical Table: 
--    CREATE TABLE shared.stefano_jiig.dag_relationships AS (<this_query>)
-- 
-- 2. Refresh Schedule: Run this query regularly (daily/hourly) to update the table
--
-- 3. Output Format: Compatible with st_link_analysis
--    - NODES: Contains job/pipeline information with failure status (success entities shown without failure data)
--    - EDGES: Contains dependency relationships
--    - Table contains ALL entities and failure data. Graph always shows all entities.
--
-- =========================

-- =========================
-- Configuration
-- =========================
-- Entity Creation Time Filtering (±3 hours from reference time)
-- Set REFERENCE_TIME to focus on entities created around a specific time
-- Use CURRENT_TIMESTAMP() for recent entities, or specify exact time
-- =========================
-- (1) 전체 엔터티 수집 (JOB + PIPELINE)
--     모든 JOB과 PIPELINE의 메타데이터 수집 (실패 여부 무관)
-- =========================
WITH all_entity_master AS (
  -- JOB 쪽 (creation time filtered: ±3 hours from reference time)
  SELECT
    'JOB'                         AS entity_type,
    j.job_id                      AS entity_id,
    j.name                        AS entity_name,
    j.description                 AS entity_description,
    j.creator_id                  AS entity_creator_id,   -- 이메일 매핑 필요
    j.run_as                      AS entity_run_as_id,    -- 이메일 매핑 필요
    NULL                          AS entity_creator_email,
    NULL                          AS entity_run_as_email,
    j.change_time                AS entity_created_time
  FROM system.lakeflow.jobs j
  WHERE array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false
    AND j.change_time >= REFERENCE_TIME - TIME_WINDOW_HOURS
    AND j.change_time <= REFERENCE_TIME + TIME_WINDOW_HOURS

  UNION ALL

  -- PIPELINE 쪽 (creation time filtered: ±3 hours from reference time)
  SELECT
    'PIPELINE'                    AS entity_type,
    p.pipeline_id                 AS entity_id,
    p.name                        AS entity_name,
    p.pipeline_type               AS entity_description,  -- pipelines에는 별도 description 미정 => 유형을 설명용으로 사용
    NULL                          AS entity_creator_id,
    NULL                          AS entity_run_as_id,
    p.created_by                  AS entity_creator_email,  -- 이미 이메일
    p.run_as                      AS entity_run_as_email,   -- 이미 이메일
    p.change_time                AS entity_created_time
  FROM system.lakeflow.pipelines p
  WHERE p.change_time >= REFERENCE_TIME - TIME_WINDOW_HOURS
    AND p.change_time <= REFERENCE_TIME + TIME_WINDOW_HOURS
),

-- =========================
-- (2) JOB 실패 추적
-- =========================
failed_jobs AS (
  SELECT DISTINCT
    job_id,
    MAX(COALESCE(period_end_time, period_start_time)) AS last_failed_time,
    MIN(COALESCE(period_start_time, period_end_time)) AS first_failed_time,
    COUNT(*) AS failure_count
  FROM system.lakeflow.job_run_timeline
  WHERE 1=1
    AND workspace_id = TARGET_WORKSPACE_ID
    AND result_state IN ('FAILED','ERROR','TIMED_OUT')
    AND run_type = 'JOB_RUN'
  GROUP BY job_id
),

-- =========================
-- (3) PIPELINE 실패 추적
--     LDP_ERR_TBL의 last_error를 실패 시각으로 사용
-- =========================
failed_pipelines AS (
  SELECT
    pipeline_id,
    MAX(last_error) AS last_failed_time,
    MIN(last_error) AS first_failed_time,
    COUNT(*) AS failure_count
  FROM identifier(LDP_ERR_TBL)
  GROUP BY pipeline_id
),

-- =========================
-- (4) 단일 Lineage 뷰 (JOB + PIPELINE)
--     entity_type 기준으로 JOB은 job_id, PIPELINE은 dlt_pipeline_id 사용
-- =========================
lineage AS (
  SELECT
    event_time,
    entity_type,                         -- 'JOB' or 'PIPELINE'
    entity_id,
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type,
    entity_metadata.job_info.job_id                      AS job_id,
    entity_metadata.job_info.job_run_id                  AS job_run_id,
    entity_metadata.dlt_pipeline_info.dlt_pipeline_id    AS dlt_pipeline_id,
    entity_metadata.dlt_pipeline_info.dlt_update_id      AS dlt_update_id
  FROM system.access.table_lineage
  WHERE 1=1
    AND workspace_id = TARGET_WORKSPACE_ID
    AND entity_type IN ('JOB','PIPELINE')
),

-- =========================
-- (5) 엔터티별 생산 테이블 (타깃 테이블)
--     각 엔터티가 생산(쓰기)하는 테이블들
-- =========================
entity_target_tables AS (
  SELECT DISTINCT
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    l.target_table_full_name
  FROM all_entity_master aem
  JOIN lineage l
    ON (
         (aem.entity_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id           = aem.entity_id)
      OR (aem.entity_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id  = aem.entity_id)
       )
  WHERE l.target_table_full_name IS NOT NULL
),

-- =========================
-- (6) 엔터티별 소스 테이블 (읽는 테이블)
--     각 엔터티가 소비(읽기)하는 테이블들
-- =========================
entity_source_tables AS (
  SELECT DISTINCT
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    l.source_table_full_name
  FROM all_entity_master aem
  JOIN lineage l
    ON (
         (aem.entity_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id           = aem.entity_id)
      OR (aem.entity_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id  = aem.entity_id)
       )
  WHERE l.source_table_full_name IS NOT NULL
),

-- =========================
-- (7) 엔터티 간 직접 관계 (Dependencies)
--     엔터티 A의 타깃 테이블을 엔터티 B가 소스로 사용하는 경우
-- =========================
entity_relationships AS (
  SELECT DISTINCT
    -- 상위 엔터티 (Producer)
    ett.entity_type         AS producer_type,
    ett.entity_id           AS producer_id,
    ett.entity_name         AS producer_name,
    
    -- 하위 엔터티 (Consumer)
    est.entity_type         AS consumer_type,
    est.entity_id           AS consumer_id,
    est.entity_name         AS consumer_name,
    
    -- 연결 테이블
    ett.target_table_full_name AS connecting_table
  FROM entity_target_tables ett
  JOIN entity_source_tables est
    ON ett.target_table_full_name = est.source_table_full_name
  WHERE NOT (
    ett.entity_type = est.entity_type 
    AND ett.entity_id = est.entity_id
  )  -- 자기 자신 제외
),

-- =========================
-- (8) 엔터티 메타데이터 완성 (이메일 매핑 포함)
-- =========================
entity_master_complete AS (
  SELECT
    aem.entity_type,
    aem.entity_id,
    aem.entity_name,
    aem.entity_description,
    COALESCE(idmap_creator.email, aem.entity_creator_email) AS entity_creator_email,
    COALESCE(idmap_runas.email,   aem.entity_run_as_email)  AS entity_run_as_email,
    -- 실패 상태 추가
    CASE 
      WHEN fj.job_id IS NOT NULL THEN true
      WHEN fp.pipeline_id IS NOT NULL THEN true
      ELSE false
    END AS is_failed,
    COALESCE(fj.last_failed_time, fp.last_failed_time) AS last_failed_time,
    COALESCE(fj.first_failed_time, fp.first_failed_time) AS first_failed_time,
    COALESCE(fj.failure_count, fp.failure_count, 0) AS failure_count,
    aem.entity_created_time AS entity_created_time,
    -- 원본 ID 보존 (st_link_analysis 호환성)
    CASE 
      WHEN aem.entity_type = 'JOB' THEN aem.entity_id
      ELSE NULL 
    END AS original_job_id,
    CASE 
      WHEN aem.entity_type = 'PIPELINE' THEN aem.entity_id
      ELSE NULL 
    END AS original_pipeline_id
  FROM all_entity_master aem
  LEFT JOIN identifier(UID_MAP_TBL) idmap_creator
    ON aem.entity_creator_id = idmap_creator.id
  LEFT JOIN identifier(UID_MAP_TBL) idmap_runas
    ON aem.entity_run_as_id = idmap_runas.id
  LEFT JOIN failed_jobs fj
    ON aem.entity_type = 'JOB' AND aem.entity_id = fj.job_id
  LEFT JOIN failed_pipelines fp
    ON aem.entity_type = 'PIPELINE' AND aem.entity_id = fp.pipeline_id
),

-- =========================
-- (9) DAG 노드 테이블 - st_link_analysis 호환
--     각 엔터티를 DAG의 노드로 표현
-- =========================
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
    -- DAG 시각화를 위한 추가 속성
    CASE 
      WHEN is_failed THEN 'FAILED'
      ELSE 'HEALTHY'
    END                              AS node_status,
    -- st_link_analysis 호환을 위한 label 필드
    upper(entity_type)               AS node_label
  FROM entity_master_complete
),

-- =========================
-- (10) DAG 엣지 테이블 - st_link_analysis 호환
--      엔터티 간 관계를 DAG의 엣지로 표현
-- =========================
dag_edges AS (
  SELECT
    producer_id                      AS source_node_id,
    lower(producer_type)             AS source_node_type,
    producer_name                    AS source_node_name,
    consumer_id                      AS target_node_id,
    lower(consumer_type)             AS target_node_type,
    consumer_name                    AS target_node_name,
    connecting_table                 AS edge_table,
    CONCAT(producer_id, '->', consumer_id) AS edge_id,
    -- st_link_analysis 호환을 위한 label 필드
    'DEPENDENCY'                     AS edge_label
  FROM entity_relationships
)

-- =========================
-- (11) 최종 결과: st_link_analysis 호환 포맷
--      노드와 엣지 정보를 st_link_analysis에서 사용하기 쉬운 형태로 제공
-- =========================
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

