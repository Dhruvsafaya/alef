CREATE TABLE dim_avatar (
  avatar_dw_id BIGINT NOT NULL,
  avatar_id VARCHAR(36) NOT NULL,
  avatar_file_id VARCHAR(36) NOT NULL,
  avatar_created_time TIMESTAMP,
  avatar_deleted_time TIMESTAMP,
  avatar_dw_created_time TIMESTAMP,
  avatar_updated_time TIMESTAMP,
  avatar_dw_updated_time TIMESTAMP,
  avatar_app_status VARCHAR(20),
  avatar_status INT,
  avatar_type VARCHAR(36),
  avatar_name VARCHAR(256),
  avatar_description VARCHAR(36),
  avatar_valid_from TIMESTAMP,
  avatar_valid_till TIMESTAMP,
  avatar_category VARCHAR(20),
  avatar_star_cost INT
);