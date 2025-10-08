CREATE TABLE IF NOT EXISTS dim_course_ability_test_association (
    cata_dw_id BIGINT,
    cata_created_time TIMESTAMP,
    cata_updated_time TIMESTAMP,
    cata_dw_created_time TIMESTAMP,
    cata_dw_updated_time TIMESTAMP,
    cata_course_id varchar(36),
    cata_ability_test_activity_uuid varchar(36),
    cata_ability_test_activity_id INT,
    cata_ability_test_id varchar(36),
    cata_max_attempts INT,
    cata_ability_test_pacing VARCHAR(16),
    cata_ability_test_index INT,
    cata_course_version VARCHAR(10),
    cata_ability_test_type INT,
    cata_status INT,
    cata_attach_status INT
);

ALTER TABLE dim_course_ability_test_association ADD COLUMN cata_ability_test_activity_type VARCHAR(50);
