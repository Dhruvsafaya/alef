CREATE TABLE IF NOT EXISTS dim_school_content_repository_association(
    scra_dw_id BIGINT,
    scra_school_id VARCHAR(36),
    scra_school_dw_id BIGINT,
    scra_content_repository_id VARCHAR(36),
    scra_content_repository_dw_id BIGINT,
    scra_status INT,
    scra_active_until TIMESTAMP,
    scra_created_time TIMESTAMP,
    scra_dw_created_time TIMESTAMP
);