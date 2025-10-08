CREATE TABLE fact_activity_setting_v2
(
    fas_activity_dw_id         BIGINT,
    fas_activity_id            VARCHAR(36),
    fas_class_dw_id            BIGINT,
    fas_class_id               VARCHAR(36),
    fas_created_time           TIMESTAMP,
    fas_dw_created_time        TIMESTAMP,
    fas_dw_id                  BIGINT,
    fas_grade_dw_id            BIGINT,
    fas_grade_id               VARCHAR(36),
    fas_k12_grade              INT,
    fas_open_path_enabled      BOOLEAN,
    fas_school_dw_id           BIGINT,
    fas_school_id              VARCHAR(36),
    fas_class_gen_subject_name VARCHAR(50),
    fas_teacher_dw_id          BIGINT,
    fas_teacher_id             VARCHAR(36),
    fas_tenant_dw_id           BIGINT,
    fas_tenant_id              VARCHAR(36)
);