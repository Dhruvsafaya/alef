CREATE TABLE IF NOT EXISTS dim_marketplace_config (
  dw_id BIGINT,
  _trace_id VARCHAR(36),
  tenant_id VARCHAR(36),
  event_type VARCHAR(50),
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  deleted_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  dw_updated_time TIMESTAMP,
  status INT,
  id VARCHAR(36),
 impact_type VARCHAR(50),
 star_cost BIGINT,
 quota BIGINT
);
