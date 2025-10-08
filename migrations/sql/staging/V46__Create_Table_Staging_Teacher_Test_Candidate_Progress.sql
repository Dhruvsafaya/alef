CREATE TABLE IF NOT EXISTS staging_teacher_test_candidate_progress
(
    fttcp_dw_id bigint,
    fttcp_session_id varchar(36),
    fttcp_candidate_id varchar(36),
    fttcp_test_delivery_id varchar(36),
    fttcp_assessment_id varchar(36),
    fttcp_score DOUBLE PRECISION,
    fttcp_stars_awarded int,
    fttcp_status varchar(50),
    fttcp_updated_at timestamp,
    fttcp_created_at timestamp,
    fttcp_date_dw_id bigint,
    fttcp_created_time timestamp,
    fttcp_dw_created_time timestamp
);