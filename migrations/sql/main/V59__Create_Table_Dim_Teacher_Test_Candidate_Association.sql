CREATE TABLE IF NOT EXISTS dim_teacher_test_candidate_association
(
    ttca_dw_id bigint,
    ttca_test_delivery_id varchar(36),
    ttca_test_id varchar(36),
    ttca_test_candidate_id varchar(36),
    ttca_status int,
    ttca_active_until timestamp,
    ttca_created_time timestamp,
    ttca_dw_created_time timestamp
)