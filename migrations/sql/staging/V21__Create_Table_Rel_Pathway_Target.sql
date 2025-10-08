CREATE TABLE rel_pathway_target
(
  rel_pt_dw_id  bigint,
  pt_dw_id  bigint,
  pt_id VARCHAR(36),
  pt_created_time Timestamp,
  pt_dw_created_time  Timestamp,
  pt_status  Int,
  pt_active_until  Timestamp,
  pt_target_id  VARCHAR(36),
  pt_target_state VARCHAR(20),
  pt_start_date VARCHAR(10),
  pt_end_date VARCHAR(10),
  pt_tenant_id  VARCHAR(36),
  pt_school_id  VARCHAR(36),
  pt_grade_id VARCHAR(36),
  pt_class_id VARCHAR(36),
  pt_teacher_id VARCHAR(36),
  pt_pathway_id VARCHAR(36)
);
