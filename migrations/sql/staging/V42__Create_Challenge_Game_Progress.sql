CREATE TABLE staging_challenge_game_progress (
    fgc_dw_id              BIGINT,
    fgc_created_time       TIMESTAMP,
    fgc_dw_created_time    TIMESTAMP,
    fgc_date_dw_id         BIGINT,
    fgc_id                 VARCHAR(36),
    fgc_game_id            VARCHAR(36),
    fgc_state              VARCHAR(20),
    fgc_tenant_id          VARCHAR(36),
    fgc_student_id         VARCHAR(36),
    fgc_academic_year_id   VARCHAR(36),
    fgc_academic_year_tag  VARCHAR(10),
    fgc_school_id          VARCHAR(36),
    fgc_grade              INT,
    fgc_organization       VARCHAR(100),
    fgc_score              INT
);
