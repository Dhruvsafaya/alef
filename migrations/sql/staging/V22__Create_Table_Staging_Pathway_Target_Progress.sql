CREATE TABLE staging_pathway_target_progress
(
  fptp_dw_id bigint,
  fptp_id VARCHAR(36),
  fptp_created_time TIMESTAMP,
  fptp_dw_created_time TIMESTAMP,
  fptp_date_dw_id BIGINT,
  fptp_student_target_id VARCHAR(36),
  fptp_target_id VARCHAR(36),
  fptp_student_id  VARCHAR(36),
  fptp_tenant_id VARCHAR(36),
  fptp_school_id VARCHAR(36),
  fptp_grade_id  VARCHAR(36),
  fptp_class_id  VARCHAR(36),
  fptp_teacher_id  VARCHAR(36),
  fptp_pathway_id  VARCHAR(36),
  fptp_target_state  VARCHAR(20),
  fptp_recommended_target_level INT,
  fptp_finalized_target_level INT,
  fptp_levels_completed INT,
  fptp_earned_stars INT
);