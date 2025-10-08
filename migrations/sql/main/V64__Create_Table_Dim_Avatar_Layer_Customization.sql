CREATE TABLE IF NOT EXISTS dim_avatar_layer_customization (
ala_dw_id BIGINT NOT NULL,
ala_created_time TIMESTAMP NOT NULL,
ala_dw_created_time TIMESTAMP NOT NULL,
ala_deleted_time TIMESTAMP,
ala_updated_time TIMESTAMP,
ala_dw_updated_time TIMESTAMP,
ala_status INT NOT NULL,
ala_avatar_id VARCHAR(36) NOT NULL,
ala_layer_id VARCHAR(36) NOT NULL
);