CREATE TABLE dim_course_resource_activity_grade_association
(
    craga_dw_id                      BIGINT,
    craga_created_time               TIMESTAMP,
    craga_updated_time               TIMESTAMP,
    craga_deleted_time               TIMESTAMP,
    craga_dw_created_time            TIMESTAMP,
    craga_dw_updated_time            TIMESTAMP,
    craga_dw_deleted_time            TIMESTAMP,
    craga_status                     INT,
    craga_course_dw_id               BIGINT,
    craga_course_id                  VARCHAR(36),
    craga_activity_dw_id             BIGINT,
    craga_activity_id                VARCHAR(36),
    craga_grade_id                   BIGINT
);

CREATE TABLE dim_course_resource_activity_outcome_association
(
    craoa_dw_id                      BIGINT,
    craoa_created_time               TIMESTAMP,
    craoa_updated_time               TIMESTAMP,
    craoa_dw_created_time            TIMESTAMP,
    craoa_dw_updated_time            TIMESTAMP,
    craoa_status                     INT,
    craoa_course_dw_id               BIGINT,
    craoa_course_id                  VARCHAR(36),
    craoa_activity_dw_id             BIGINT,
    craoa_activity_id                VARCHAR(36),
    craoa_outcome_id                 VARCHAR(36),
    craoa_outcome_type               VARCHAR(50),
    craoa_curr_id                    BIGINT,
    craoa_curr_grade_id              BIGINT,
    craoa_curr_subject_id            BIGINT
);