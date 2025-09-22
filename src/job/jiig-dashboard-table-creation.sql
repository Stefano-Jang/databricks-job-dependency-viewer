-- =========================
-- Configuration
-- =========================
DECLARE OR REPLACE UID_MAP_TBL STRING DEFAULT {{user_id_map_table_name}};
DECLARE OR REPLACE LDP_ERR_TBL STRING DEFAULT {{ldp_error_table_full_name}};
DECLARE OR REPLACE TARGET_WORKSPACE_ID STRING DEFAULT {{workspace_id}};
DECLARE OR REPLACE DASHBOARD_TABLE STRING DEFAULT {{dashboard_table}};

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

-- =========================
-- (1) 실패 런 수집 (JOB)
-- =========================
CREATE OR REPLACE TABLE identifier(DASHBOARD_TABLE) AS (
WITH fail_runs_job AS (
  SELECT DISTINCT
    job_id,
    run_id,
    period_start_time,
    period_end_time,
    trigger_type,
    result_state,
    termination_code,
    COALESCE(period_end_time, period_start_time) AS failed_at
  FROM system.lakeflow.job_run_timeline
  WHERE 1=1
    AND workspace_id = TARGET_WORKSPACE_ID
    AND result_state IN ('FAILED','ERROR','TIMED_OUT')
    AND run_type = 'JOB_RUN'
),

failed_jobs_last AS (
  SELECT
    job_id,
    MAX(failed_at) AS last_failed_time
  FROM fail_runs_job
  GROUP BY job_id
),

-- =========================
-- (2) 실패 런 수집 (PIPELINE)
--    LDP_ERR_TBL의 last_error를 실패 시각으로 사용, 별도 노트북 job 수행 필요
-- =========================
failed_pipelines_last AS (
  SELECT
    pipeline_id,
    MAX(last_error) AS last_failed_time
  FROM identifier(LDP_ERR_TBL)
  GROUP BY pipeline_id
),

-- =========================
-- (3) 실패 엔터티 마스터 (JOB + PIPELINE)
--     * JOB: jobs 테이블에서 메타데이터 가져옴 (tags 필터 포함)
--     * PIPELINE: pipelines 테이블에서 메타데이터 가져옴 (created_by/run_as는 email 그대로 사용)
-- =========================
failed_entity_master AS (
  -- JOB 쪽
  SELECT
    'JOB'                         AS failed_type,
    j.job_id                      AS failed_id,
    j.name                        AS failed_name,
    j.description                 AS failed_description,
    j.creator_id                  AS failed_creator_id,   -- 이메일 매핑 필요
    j.run_as                      AS failed_run_as_id,    -- 이메일 매핑 필요
    (select max(email) from identifier(UID_MAP_TBL) where id = j.creator_id) AS failed_creator_email,
    (select max(email) from identifier(UID_MAP_TBL) where id = j.run_as) AS failed_run_as_email,
    fjl.last_failed_time          AS last_failed_time
  FROM system.lakeflow.jobs j
  JOIN failed_jobs_last fjl ON j.job_id = fjl.job_id
  WHERE array_contains(map_keys(j.tags), 'LakehouseMonitoringAnomalyDetection') = false

  UNION ALL

  -- PIPELINE 쪽
  SELECT
    'PIPELINE'                    AS failed_type,
    p.pipeline_id                 AS failed_id,
    p.name                        AS failed_name,
    p.pipeline_type               AS failed_description,  -- pipelines에는 별도 description 미정 => 유형을 설명용으로 사용
    NULL                          AS failed_creator_id,
    NULL                          AS failed_run_as_id,
    p.created_by                  AS failed_creator_email,  -- 이미 이메일
    p.run_as                      AS failed_run_as_email,   -- 이미 이메일
    fpl.last_failed_time          AS last_failed_time
  FROM system.lakeflow.pipelines p
  JOIN failed_pipelines_last fpl ON p.pipeline_id = fpl.pipeline_id
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
-- (5) 실패 엔터티가 "생산(쓰기)"한 타깃 테이블
--     entity_type/ID로 lineage 매칭
-- =========================
failed_entity_target_tables AS (
  SELECT DISTINCT
    fem.failed_type,
    fem.failed_id,
    fem.failed_name,
    fem.last_failed_time,
    fem.failed_creator_email,
    fem.failed_run_as_email,
    l.target_table_full_name AS target_table_full_name
  FROM failed_entity_master fem
  JOIN lineage l
    ON (
         (fem.failed_type = 'JOB'      AND l.entity_type = 'JOB'      AND l.job_id           = fem.failed_id)
      OR (fem.failed_type = 'PIPELINE' AND l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id  = fem.failed_id)
       )
  WHERE l.target_table_full_name IS NOT NULL
),

-- =========================
-- (6) 해당 타깃 테이블을 읽는 "영향 엔터티"(JOB 또는 PIPELINE)
-- =========================
affected_entities AS (
  SELECT DISTINCT
    fett.failed_type,
    fett.failed_id,
    fett.failed_name,
    fett.last_failed_time,
    fett.failed_creator_email,
    fett.failed_run_as_email,
    fett.target_table_full_name              AS affected_table,
    l.entity_type                            AS affected_type,  -- 'JOB' or 'PIPELINE'
    COALESCE(l.job_id, l.dlt_pipeline_id)    AS affected_id
  FROM failed_entity_target_tables fett
  JOIN lineage l
    ON fett.target_table_full_name = l.source_table_full_name
  WHERE l.entity_type IN ('JOB','PIPELINE')
    AND NOT (
      (fett.failed_type = l.entity_type)
      AND (
            (l.entity_type = 'JOB'      AND l.job_id          = fett.failed_id)
         OR (l.entity_type = 'PIPELINE' AND l.dlt_pipeline_id = fett.failed_id)
          )
    )  -- 자기 자신 제외
),

-- =========================
-- (7) 영향 엔터티 메타데이터 (JOB)
-- =========================
affected_job_details AS (
  SELECT
    ae.failed_type,
    ae.failed_id,
    ae.failed_name,
    ae.last_failed_time,
    ae.failed_creator_email,
    ae.failed_run_as_email,
    ae.affected_table,
    'JOB'                         AS affected_type,
    ae.affected_id                AS affected_id,
    j.name                        AS affected_name,
    j.description                 AS affected_description,
    j.creator_id                  AS affected_creator_id,
    j.run_as                      AS affected_run_as_id,
    NULL                          AS affected_creator_email,  -- 매핑 필요
    NULL                          AS affected_run_as_email    -- 매핑 필요
  FROM affected_entities ae
  JOIN system.lakeflow.jobs j
    ON ae.affected_type = 'JOB' AND ae.affected_id = j.job_id
),

-- =========================
-- (8) 영향 엔터티 메타데이터 (PIPELINE)
--     created_by/run_as는 이메일이므로 그대로 사용
-- =========================
affected_pipeline_details AS (
  SELECT
    ae.failed_type,
    ae.failed_id,
    ae.failed_name,
    ae.last_failed_time,
    ae.failed_creator_email,
    ae.failed_run_as_email,
    ae.affected_table,
    'PIPELINE'                    AS affected_type,
    ae.affected_id                AS affected_id,
    p.name                        AS affected_name,
    p.pipeline_type               AS affected_description,  -- 설명 역할
    NULL                          AS affected_creator_id,
    NULL                          AS affected_run_as_id,
    p.created_by                  AS affected_creator_email,
    p.run_as                      AS affected_run_as_email
  FROM affected_entities ae
  JOIN system.lakeflow.pipelines p
    ON ae.affected_type = 'PIPELINE' AND ae.affected_id = p.pipeline_id
),

-- =========================
-- (9) 영향 엔터티 메타데이터 통합 (JOB + PIPELINE)
--     JOB의 경우 UID 맵으로 이메일 매핑
-- =========================
affected_entity_details AS (
  SELECT
    ajd.failed_type,
    ajd.failed_id,
    ajd.failed_name,
    ajd.last_failed_time,
    ajd.failed_creator_email,
    ajd.failed_run_as_email,
    ajd.affected_table,
    ajd.affected_type,
    ajd.affected_id,
    ajd.affected_name,
    ajd.affected_description,
    COALESCE(idmap_creator.email, ajd.affected_creator_email) AS affected_creator_email,
    COALESCE(idmap_runas.email,   ajd.affected_run_as_email)  AS affected_run_as_email
  FROM (
    SELECT * FROM affected_job_details
    UNION ALL
    SELECT * FROM affected_pipeline_details
  ) ajd
  LEFT JOIN identifier(UID_MAP_TBL) idmap_creator
    ON ajd.affected_creator_id = idmap_creator.id    -- JOB만 매핑됨
  LEFT JOIN identifier(UID_MAP_TBL) idmap_runas
    ON ajd.affected_run_as_id  = idmap_runas.id      -- JOB만 매핑됨
),

-- =========================
-- (10) 최종 BASE
-- =========================
BASE AS (
SELECT
  aed.failed_id,                             -- (2) failed_job_id -> failed_id
  lower(aed.failed_type) as failed_type,                 -- (3) 실패 주체 타입 (JOB | PIPELINE)
  MAX(aed.failed_name)            AS failed_name,        -- (2) failed_job_name -> failed_name
  MAX(aed.last_failed_time)       AS last_failed_time,   -- (1) 마지막 실패 시각
  MAX(aed.failed_creator_email)   AS failed_creator_email,
  MAX(aed.failed_run_as_email)    AS failed_run_as_email,
  aed.affected_id,
  lower(aed.affected_type) as affected_type,             -- (3) 영향 주체 타입 (JOB | PIPELINE)
  MAX(aed.affected_name)         AS affected_name,
  CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(aed.affected_table)))               AS affected_tables,
  MAX(aed.affected_creator_email) AS affected_creator_email,
  MAX(aed.affected_run_as_email)  AS affected_run_as_email,
  CONCAT_WS(' || ', SORT_ARRAY(COLLECT_SET(aed.affected_description)))       AS affected_descriptions
FROM affected_entity_details aed
GROUP BY
  aed.failed_id,
  aed.failed_type,
  aed.failed_creator_email,
  aed.failed_run_as_email,
  aed.affected_id,
  aed.affected_type
ORDER BY
  MAX(aed.last_failed_time) DESC,
  aed.failed_id,
  aed.affected_id
)
select * from BASE
);