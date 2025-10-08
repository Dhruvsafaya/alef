CREATE TABLE IF NOT EXISTS dim_test_testpart_association (
  dw_id BIGINT,
  _trace_id VARCHAR(36),
  tenant_id VARCHAR(36),
  event_type VARCHAR(50),
  created_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  active_until TIMESTAMP,
  status INT,
  id VARCHAR(36),
  version_id VARCHAR(36),
  version BIGINT,
  title VARCHAR(768),
  testpart_id VARCHAR(36),
  testpart_version_id VARCHAR(36),
  app_status VARCHAR(36),
  user_type VARCHAR(36),
  active BOOLEAN
);
