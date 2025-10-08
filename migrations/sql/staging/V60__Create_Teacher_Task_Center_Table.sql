CREATE TABLE IF NOT EXISTS staging_teacher_task_center (
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
  school_id VARCHAR(36),
  class_id VARCHAR(36),
  teacher_id VARCHAR(36)
);
