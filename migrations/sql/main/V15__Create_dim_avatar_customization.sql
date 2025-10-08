CREATE TABLE dim_avatar_customization (
  ac_dw_id BIGINT NOT NULL,
  ac_created_time TIMESTAMP,
  ac_updated_time TIMESTAMP,
  ac_deleted_time TIMESTAMP,
  ac_dw_created_time TIMESTAMP,
  ac_dw_updated_time TIMESTAMP,
  ac_status INT,
  ac_student_id VARCHAR(36),
  ac_student_dw_id INT,
  ac_avatar_id VARCHAR(36),
  ac_avatar_dw_id INT,
  ac_type VARCHAR(30),
  ac_item_id VARCHAR(36),
  ac_item_type VARCHAR(30),
  ac_item_value VARCHAR(60)
);