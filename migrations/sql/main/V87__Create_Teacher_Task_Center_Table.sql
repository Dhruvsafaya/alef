CREATE TABLE IF NOT EXISTS fact_teacher_task_center (
  dw_id BIGINT,
  created_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  date_dw_id BIGINT,
  _trace_id VARCHAR(36),
  event_type VARCHAR(36),
  event_id VARCHAR(36),
  task_id VARCHAR(36),
  task_type VARCHAR(36),
  tenant_id VARCHAR(36),
  tenant_dw_id bigint,
  school_id VARCHAR(36),
  school_dw_id bigint,
  class_id VARCHAR(36),
  class_dw_id bigint,
  teacher_id VARCHAR(36),
  teacher_dw_id bigint
);
