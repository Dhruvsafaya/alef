CREATE SCHEMA IF NOT EXISTS alefdw;
CREATE SCHEMA IF NOT EXISTS alefdw_stage;

CREATE TABLE alefdw.dim_grade
(
  grade_dw_id BIGINT IDENTITY(1,1),
  grade_created_time TIMESTAMP,
  grade_updated_time TIMESTAMP,
  grade_deleted_time TIMESTAMP,
  grade_dw_created_time TIMESTAMP,
  grade_dw_updated_time TIMESTAMP,
  grade_status INT,

  grade_id VARCHAR(36),
  grade_name VARCHAR(250),
  grade_k12grade INT

)
diststyle all
compound sortkey(grade_dw_id);

CREATE TABLE alefdw.dim_school
(
  school_dw_id BIGINT IDENTITY(1,1),
  school_created_time TIMESTAMP,
  school_updated_time TIMESTAMP,
  school_deleted_time TIMESTAMP,
  school_dw_created_time TIMESTAMP,
  school_dw_updated_time TIMESTAMP,
  school_status INT,

  school_id VARCHAR(36),
  school_name VARCHAR(256),
  school_organisation VARCHAR(255),
  school_address_line VARCHAR(255),
  school_post_box VARCHAR(10),
  school_city_name VARCHAR(100),
  school_country_name VARCHAR(100),
  school_latitude DECIMAL(10,6),
  school_longitude DECIMAL(10,6),
  school_first_day VARCHAR(30),
  school_timezone VARCHAR(64),
  school_composition VARCHAR(64)
)
diststyle all
compound sortkey(school_dw_id);

CREATE TABLE alefdw.dim_subject
(
  subject_dw_id BIGINT IDENTITY(1,1),
  subject_created_time TIMESTAMP,
  subject_updated_time TIMESTAMP,
  subject_deleted_time TIMESTAMP,
  subject_dw_created_time TIMESTAMP,
  subject_dw_updated_time TIMESTAMP,
  subject_status INT,

  subject_id VARCHAR(36),
  subject_name VARCHAR(255),
  subject_online boolean,
  subject_gen_subject VARCHAR(255)
)
diststyle all
compound sortkey(subject_dw_id);

CREATE TABLE alefdw.dim_learning_objective
(
  lo_dw_id BIGINT IDENTITY(1,1),
  lo_created_time TIMESTAMP,
  lo_updated_time TIMESTAMP,
  lo_deleted_time TIMESTAMP,
  lo_dw_created_time TIMESTAMP,
  lo_dw_updated_time TIMESTAMP,
  lo_status INT,

  lo_id VARCHAR(36),
  lo_title VARCHAR(255),
  lo_code VARCHAR(50),
  lo_type VARCHAR(50)
)
diststyle all
compound sortkey(lo_dw_id);

CREATE TABLE alefdw.dim_learning_path
(
  learning_path_dw_id BIGINT IDENTITY(1,1),
  learning_path_created_time TIMESTAMP,
  learning_path_updated_time TIMESTAMP,
  learning_path_deleted_time TIMESTAMP,
  learning_path_dw_created_time TIMESTAMP,
  learning_path_dw_updated_time TIMESTAMP,
  learning_path_status INT,

  learning_path_id VARCHAR(108),
  learning_path_uuid VARCHAR(36),
  learning_path_name VARCHAR(255),
  learning_path_lp_status VARCHAR(50),
  learning_path_language_type_script VARCHAR(50),
  learning_path_experiential_learning boolean,
  learning_path_tutor_dhabi_enabled boolean,
  learning_path_default boolean,
  learning_path_school_id varchar (36),
  learning_path_subject_id VARCHAR(36),
  learning_path_curriculum_id VARCHAR(50),
  learning_path_curriculum_grade_id VARCHAR(50),
  learning_path_curriculum_subject_id VARCHAR(50)
)
diststyle all
compound sortkey(learning_path_dw_id);

CREATE TABLE alefdw.dim_skill
(
  skill_dw_id BIGINT IDENTITY(1,1),
  skill_created_time TIMESTAMP,
  skill_updated_time TIMESTAMP,
  skill_deleted_time TIMESTAMP,
  skill_dw_created_time TIMESTAMP,
  skill_dw_updated_time TIMESTAMP,
  skill_status INT,

  skill_id VARCHAR(36),
  skill_name VARCHAR(500),
  skill_code VARCHAR(150)
)
diststyle all
compound sortkey(skill_dw_id);

CREATE TABLE alefdw.dim_academic_year
(
  academic_year_dw_id BIGINT IDENTITY(1,1),
  academic_year_created_time TIMESTAMP,
  academic_year_updated_time TIMESTAMP,
  academic_year_deleted_time TIMESTAMP,
  academic_year_dw_created_time TIMESTAMP,
  academic_year_dw_updated_time TIMESTAMP,
  academic_year_status SMALLINT,

  academic_year_id VARCHAR(36),
  academic_year_school_id VARCHAR(36),
  academic_year_start_date DATE ,
  academic_year_end_date DATE
)
diststyle all
compound sortkey(academic_year_dw_id);


CREATE TABLE alefdw_stage.rel_academic_year
(
    academic_year_delta_dw_id BIGINT,
    academic_year_created_time TIMESTAMP,
    academic_year_updated_time TIMESTAMP,
    academic_year_deleted_time TIMESTAMP,
    academic_year_dw_created_time TIMESTAMP,
    academic_year_dw_updated_time TIMESTAMP,
    academic_year_status SMALLINT,
    academic_year_id VARCHAR(36),
    academic_year_organization_code VARCHAR(50),
    academic_year_school_id VARCHAR(36),
    academic_year_state VARCHAR(50),
    academic_year_start_date DATE,
    academic_year_end_date DATE,
    academic_year_created_by VARCHAR(36),
    academic_year_updated_by VARCHAR(36),
    academic_year_is_roll_over_completed BOOLEAN DEFAULT false
);

ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_delta_dw_id BIGINT;
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_organization_code VARCHAR(50);
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_organization_dw_id BIGINT;
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_school_dw_id BIGINT;
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_created_by_dw_id BIGINT;
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_updated_by_dw_id BIGINT;
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_state VARCHAR(50);
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_created_by VARCHAR(36);
ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_updated_by VARCHAR(36);


CREATE TABLE alefdw_stage.rel_user
(
  user_dw_id BIGINT IDENTITY(1,1),
  user_id VARCHAR(36)
)
compound sortkey(user_dw_id);

CREATE TABLE alefdw_stage.rel_student
(
  rel_student_id BIGINT IDENTITY(1,1),
  student_created_time TIMESTAMP,
  student_updated_time TIMESTAMP,
  student_deleted_time TIMESTAMP,
  student_dw_created_time TIMESTAMP,
  student_dw_updated_time TIMESTAMP,
  student_active_until TIMESTAMP,
  student_status INT,

  student_uuid VARCHAR(36),
  student_username VARCHAR(256),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  student_tags VARCHAR(256)
)
compound sortkey(school_uuid, grade_uuid, class_uuid);

CREATE TABLE alefdw.dim_student
(
  rel_student_dw_id BIGINT IDENTITY(1,1),
  student_created_time TIMESTAMP,
  student_updated_time TIMESTAMP,
  student_deleted_time TIMESTAMP,
  student_dw_created_time TIMESTAMP,
  student_dw_updated_time TIMESTAMP,
  student_active_until TIMESTAMP,
  student_status INT,

  student_id VARCHAR(36),
  student_username VARCHAR(256),
  student_dw_id BIGINT,
  student_school_dw_id BIGINT,
  student_grade_dw_id BIGINT,
  student_class_dw_id BIGINT,
  student_tags VARCHAR(256)
)
distkey(student_school_dw_id)
compound sortkey(student_school_dw_id, student_grade_dw_id, student_class_dw_id);

CREATE TABLE alefdw_stage.rel_guardian
(
  rel_guardian_id BIGINT IDENTITY(1,1),
  guardian_created_time TIMESTAMP,
  guardian_updated_time TIMESTAMP,
  guardian_deleted_time TIMESTAMP,
  guardian_dw_created_time TIMESTAMP,
  guardian_dw_updated_time TIMESTAMP,
  guardian_active_until TIMESTAMP,
  guardian_status INT,

  guardian_uuid VARCHAR(36),
  student_uuid VARCHAR(36)
)
compound sortkey(student_uuid);

CREATE TABLE alefdw.dim_guardian
(
  rel_guardian_dw_id BIGINT IDENTITY(1,1),
  guardian_created_time TIMESTAMP,
  guardian_updated_time TIMESTAMP,
  guardian_deleted_time TIMESTAMP,
  guardian_dw_created_time TIMESTAMP,
  guardian_dw_updated_time TIMESTAMP,
  guardian_active_until TIMESTAMP,
  guardian_status INT,

  guardian_id VARCHAR(36),
  guardian_dw_id BIGINT,
  guardian_student_dw_id BIGINT
)
distkey(guardian_student_dw_id)
compound sortkey(guardian_student_dw_id);

CREATE TABLE alefdw_stage.rel_guardian_invitation
(
  rel_guardian_invitation_id BIGINT IDENTITY(1,1),
  guardian_invitation_created_time TIMESTAMP,
  guardian_invitation_dw_created_time TIMESTAMP,

  guardian_uuid VARCHAR(36),
  guardian_invitation_status INT
);

CREATE TABLE alefdw.dim_guardian_invitation
(
  rel_guardian_invitation_id BIGINT IDENTITY(1,1),
  guardian_invitation_created_time TIMESTAMP,
  guardian_invitation_dw_created_time TIMESTAMP,

  guardian_invitation_guardian_dw_id BIGINT,
  guardian_invitation_status INT
);

CREATE TABLE alefdw_stage.rel_teacher
(
  rel_teacher_id BIGINT IDENTITY(1,1),
  teacher_created_time TIMESTAMP,
  teacher_updated_time TIMESTAMP,
  teacher_deleted_time TIMESTAMP,
  teacher_dw_created_time TIMESTAMP,
  teacher_dw_updated_time TIMESTAMP,
  teacher_active_until TIMESTAMP,
  teacher_status INT,

  teacher_uuid VARCHAR(36),
  subject_uuid VARCHAR(36)
)
compound sortkey(teacher_uuid);

CREATE TABLE alefdw.dim_teacher
(
  rel_teacher_dw_id BIGINT IDENTITY(1,1),
  teacher_created_time TIMESTAMP,
  teacher_updated_time TIMESTAMP,
  teacher_deleted_time TIMESTAMP,
  teacher_dw_created_time TIMESTAMP,
  teacher_dw_updated_time TIMESTAMP,
  teacher_active_until TIMESTAMP,
  teacher_status INT,

  teacher_id VARCHAR(36),
  teacher_dw_id BIGINT,
  teacher_subject_dw_id BIGINT
)
distkey(teacher_dw_id)
compound sortkey(teacher_dw_id);

CREATE TABLE alefdw_stage.rel_tdc
(
  rel_tdc_id BIGINT IDENTITY(1,1),
  tdc_created_time TIMESTAMP,
  tdc_updated_time TIMESTAMP,
  tdc_deleted_time TIMESTAMP,
  tdc_dw_created_time TIMESTAMP,
  tdc_dw_updated_time TIMESTAMP,
  tdc_active_until TIMESTAMP,
  tdc_status INT,

  tdc_uuid VARCHAR(36),
  tdc_avatar VARCHAR(100),
  tdc_onboarded BOOLEAN,
  school_uuid VARCHAR(36),
  tdc_expirable BOOLEAN
)
compound sortkey(tdc_uuid);

CREATE TABLE alefdw.dim_tdc
(
  rel_tdc_dw_id BIGINT IDENTITY(1,1),
  tdc_created_time TIMESTAMP,
  tdc_updated_time TIMESTAMP,
  tdc_deleted_time TIMESTAMP,
  tdc_dw_created_time TIMESTAMP,
  tdc_dw_updated_time TIMESTAMP,
  tdc_active_until TIMESTAMP,
  tdc_status INT,

  tdc_id VARCHAR(36),
  tdc_dw_id BIGINT,
  tdc_avatar VARCHAR(100),
  tdc_onboarded BOOLEAN,
  tdc_school_dw_id VARCHAR(36),
  tdc_expirable BOOLEAN
)
distkey(tdc_school_dw_id)
compound sortkey(tdc_dw_id);


CREATE TABLE alefdw_stage.rel_principal
(
  rel_principal_id BIGINT IDENTITY(1,1),
  principal_created_time TIMESTAMP,
  principal_updated_time TIMESTAMP,
  principal_deleted_time TIMESTAMP,
  principal_dw_created_time TIMESTAMP,
  principal_dw_updated_time TIMESTAMP,
  principal_active_until TIMESTAMP,
  principal_status INT,

  principal_uuid VARCHAR(36),
  principal_avatar VARCHAR(100),
  principal_onboarded BOOLEAN,
  school_uuid VARCHAR(36),
  principal_expirable BOOLEAN
)
compound sortkey(principal_uuid);

CREATE TABLE alefdw.dim_principal
(
  rel_principal_dw_id BIGINT IDENTITY(1,1),
  principal_created_time TIMESTAMP,
  principal_updated_time TIMESTAMP,
  principal_deleted_time TIMESTAMP,
  principal_dw_created_time TIMESTAMP,
  principal_dw_updated_time TIMESTAMP,
  principal_active_until TIMESTAMP,
  principal_status INT,

  principal_id VARCHAR(36),
  principal_dw_id BIGINT,
  principal_avatar VARCHAR(100),
  principal_onboarded BOOLEAN,
  principal_school_dw_id VARCHAR(36),
  principal_expirable BOOLEAN
)
distkey(principal_school_dw_id)
compound sortkey(principal_dw_id);


CREATE TABLE alefdw_stage.staging_learning_experience
(
  fle_staging_id BIGINT IDENTITY(1,1),
  fle_created_time TIMESTAMP,
  fle_dw_created_time TIMESTAMP,
  fle_date_dw_id BIGINT,
  fle_exp_id VARCHAR(36),
  fle_ls_id VARCHAR(36),
  fle_content_id VARCHAR(36),
  lo_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  curr_subject_uuid VARCHAR(36),
  curr_grade_uuid VARCHAR(36),
  curr_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  lp_uuid VARCHAR(36),
  fle_start_time TIMESTAMP,
  fle_end_time TIMESTAMP,
  fle_total_time DOUBLE PRECISION,
  fle_score INT,
  fle_star_earned INT,
  fle_lesson_type VARCHAR(50),
  fle_is_retry boolean,
  fle_outside_of_school boolean,
  fle_attempt INT,
  fle_exp_ls_flag boolean,
  fle_academic_period_order VARCHAR(20)
)
sortkey(school_uuid, grade_uuid, class_uuid);

CREATE TABLE alefdw.fact_learning_experience
(
  fle_dw_id BIGINT IDENTITY(1,1),
  fle_created_time TIMESTAMP,
  fle_dw_created_time TIMESTAMP,
  fle_date_dw_id BIGINT,
  fle_exp_id VARCHAR(36),
  fle_ls_id VARCHAR(36),
  fle_content_id VARCHAR(36),
  fle_lo_dw_id BIGINT,
  fle_student_dw_id BIGINT,
  fle_subject_dw_id BIGINT,
  fle_grade_dw_id BIGINT,
  fle_curr_subject_dw_id BIGINT,
  fle_curr_grade_dw_id BIGINT,
  fle_curr_dw_id BIGINT,
  fle_tenant_dw_id BIGINT,
  fle_school_dw_id BIGINT,
  fle_class_dw_id BIGINT,
  fle_lp_dw_id BIGINT,
  fle_start_time TIMESTAMP,
  fle_end_time TIMESTAMP,
  fle_total_time DOUBLE PRECISION,
  fle_score INT,
  fle_star_earned INT,
  fle_lesson_type VARCHAR(50),
  fle_is_retry boolean,
  fle_outside_of_school boolean,
  fle_attempt INT,
  fle_exp_ls_flag boolean,
  fle_academic_period_order VARCHAR(20)
)
sortkey(fle_school_dw_id, fle_grade_dw_id, fle_class_dw_id, fle_student_dw_id, fle_ls_id, fle_date_dw_id);

CREATE TABLE alefdw_stage.staging_star_awarded
(
  fsa_staging_id BIGINT IDENTITY(1,1),
  fsa_created_time TIMESTAMP,
  fsa_dw_created_time TIMESTAMP,
  fsa_date_dw_id BIGINT,
  fsa_id VARCHAR(36),
  award_category_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  teacher_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  fsa_award_comments boolean
)
sortkey(school_uuid, subject_uuid);

CREATE TABLE alefdw.fact_star_awarded
(
  fsa_dw_id BIGINT IDENTITY(1,1),
  fsa_created_time TIMESTAMP,
  fsa_dw_created_time TIMESTAMP,
  fsa_date_dw_id BIGINT,

  fsa_id VARCHAR(36),
  fsa_award_category_dw_id BIGINT,
  fsa_student_dw_id BIGINT,
  fsa_tenant_dw_id BIGINT,
  fsa_school_dw_id BIGINT,
  fsa_subject_dw_id BIGINT,
  fsa_teacher_dw_id BIGINT,
  fsa_grade_dw_id BIGINT,
  fsa_award_comments boolean
)
sortkey(fsa_school_dw_id, fsa_subject_dw_id, fsa_date_dw_id);

CREATE TABLE alefdw_stage.staging_user_login
(
  ful_staging_id BIGINT IDENTITY(1,1),
  ful_created_time TIMESTAMP,
  ful_dw_created_time TIMESTAMP,
  ful_date_dw_id BIGINT,
  ful_id VARCHAR(36),
  user_uuid VARCHAR(36),
  role_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  ful_outside_of_school boolean,
  ful_login_time TIMESTAMP
)
sortkey(school_uuid);

CREATE TABLE alefdw.fact_user_login
(
  ful_dw_id BIGINT IDENTITY(1,1),
  ful_created_time TIMESTAMP,
  ful_dw_created_time TIMESTAMP,
  ful_date_dw_id BIGINT,
  ful_id VARCHAR(36),
  ful_user_dw_id BIGINT,
  ful_role_dw_id BIGINT,
  ful_tenant_dw_id BIGINT,
  ful_school_dw_id BIGINT,
  ful_outside_of_school boolean,
  ful_login_time TIMESTAMP
)
sortkey(ful_school_dw_id, ful_date_dw_id);

CREATE TABLE alefdw_stage.staging_conversation_occurred
(
  fco_staging_id BIGINT IDENTITY(1,1),
  fco_created_time TIMESTAMP,
  fco_dw_created_time TIMESTAMP,
  fco_date_dw_id BIGINT,
  fco_id VARCHAR(36),
  student_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  subject_uuid VARCHAR (36),
  class_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  fco_question VARCHAR(2046),
  fco_answer_id VARCHAR(36),
  fco_answer VARCHAR(2046),
  fco_arabic_answer VARCHAR(2046),
  fco_source VARCHAR(256),
  fco_suggestions VARCHAR(2046)
  )
sortkey(school_uuid);

CREATE TABLE alefdw.fact_conversation_occurred
(
  fco_dw_id BIGINT IDENTITY(1,1),
  fco_created_time TIMESTAMP,
  fco_dw_created_time TIMESTAMP,
  fco_date_dw_id BIGINT,
  fco_id VARCHAR(36),
  fco_student_dw_id BIGINT,
  fco_school_dw_id BIGINT,
  fco_tenant_dw_id BIGINT,
  fco_grade_dw_id BIGINT,
  fco_subject_dw_id BIGINT,
  fco_class_dw_id BIGINT,
  fco_lo_dw_id BIGINT,
  fco_question VARCHAR(2046),
  fco_answer_id VARCHAR(36),
  fco_answer VARCHAR(2046),
  fco_arabic_answer VARCHAR(2046),
  fco_source VARCHAR(256),
  fco_suggestions VARCHAR(2046)
)
sortkey(fco_school_dw_id, fco_date_dw_id);

CREATE TABLE alefdw_stage.staging_practice
(
  practice_staging_id BIGINT IDENTITY(1,1),
  practice_created_time TIMESTAMP,
  practice_dw_created_time TIMESTAMP,
  practice_date_dw_id INT,
  practice_id VARCHAR(36),
  lo_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  skill_uuid VARCHAR(36),
  practice_sa_score DECIMAL(10,4),
  item_lo_uuid VARCHAR(36),
  item_skill_uuid VARCHAR(36),
  practice_item_content_id VARCHAR(36),
  practice_item_content_title VARCHAR(100),
  practice_item_content_lesson_type VARCHAR(50),
  practice_item_content_location VARCHAR(200)
)
sortkey(school_uuid, grade_uuid, class_uuid);

CREATE TABLE alefdw.fact_practice
(
  practice_dw_id BIGINT IDENTITY(1,1),
  practice_created_time TIMESTAMP,
  practice_dw_created_time TIMESTAMP,
  practice_date_dw_id INT,
  practice_id VARCHAR(36),
  practice_lo_dw_id BIGINT,
  practice_student_dw_id BIGINT,
  practice_subject_dw_id BIGINT,
  practice_grade_dw_id BIGINT,
  practice_tenant_dw_id BIGINT,
  practice_school_dw_id BIGINT,
  practice_class_dw_id BIGINT,
  practice_skill_dw_id BIGINT,
  practice_sa_score DECIMAL(10,4),
  practice_item_lo_dw_id BIGINT,
  practice_item_skill_dw_id BIGINT,
  practice_item_content_id VARCHAR(36),
  practice_item_content_title VARCHAR(100),
  practice_item_content_lesson_type VARCHAR(50),
  practice_item_content_location VARCHAR(200)
)
distkey(practice_date_dw_id)
sortkey(practice_school_dw_id, practice_grade_dw_id, practice_class_dw_id, practice_student_dw_id);

CREATE TABLE alefdw_stage.staging_practice_session
(
  practice_session_staging_id BIGINT IDENTITY(1,1),
  practice_session_created_time TIMESTAMP,
  practice_session_dw_created_time TIMESTAMP,
  practice_session_date_dw_id INT,
  practice_session_id VARCHAR(36),
  lo_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  skill_uuid VARCHAR(36),
  practice_session_sa_score DECIMAL(10,4),
  practice_session_item_lo_uuid VARCHAR(36) default 'n/a',
  practice_session_item_skill_uuid VARCHAR(36) default 'n/a',
  practice_session_item_content_uuid VARCHAR(36) default 'n/a',
  practice_session_item_content_title VARCHAR(100) default 'n/a',
  practice_session_item_content_lesson_type VARCHAR(50) default 'n/a',
  practice_session_item_content_location VARCHAR(200) default 'n/a',
  practice_session_score DECIMAL(10,4) default null,
  practice_session_event_type INT,
  practice_session_is_start BOOLEAN default false,
  practice_session_is_start_event_processed BOOLEAN default false
)
sortkey(school_uuid, grade_uuid, class_uuid,student_uuid);

CREATE TABLE alefdw.fact_practice_session
(
  practice_session_dw_id BIGINT IDENTITY(1,1),
  practice_session_start_time TIMESTAMP,
  practice_session_end_time TIMESTAMP,
  practice_session_dw_created_time TIMESTAMP,
  practice_session_date_dw_id INT,
  practice_session_id VARCHAR(36),
  practice_session_lo_dw_id BIGINT,
  practice_session_student_dw_id BIGINT,
  practice_session_subject_dw_id BIGINT,
  practice_session_grade_dw_id BIGINT,
  practice_session_tenant_dw_id BIGINT,
  practice_session_school_dw_id BIGINT,
  practice_session_class_dw_id BIGINT,
  practice_session_skill_dw_id BIGINT,
  practice_session_sa_score DECIMAL(10,4),
  practice_session_item_lo_dw_id BIGINT,
  practice_session_item_skill_dw_id BIGINT,
  practice_session_item_content_uuid VARCHAR(36),
  practice_session_item_content_title VARCHAR(100),
  practice_session_item_content_lesson_type VARCHAR(50),
  practice_session_item_content_location VARCHAR(200),
  practice_session_time_spent BIGINT,
  practice_session_score DECIMAL(10,4),
  practice_session_event_type INT,
  practice_session_is_start BOOLEAN
)
distkey(practice_session_date_dw_id)
sortkey(practice_session_school_dw_id, practice_session_grade_dw_id, practice_session_class_dw_id,practice_session_student_dw_id);

CREATE TABLE alefdw_stage.staging_teacher_activities
(
  fta_staging_id BIGINT IDENTITY(1,1),
  fta_created_time TIMESTAMP,
  fta_dw_created_time TIMESTAMP,
  fta_actor_object_type VARCHAR(100),
  fta_actor_account_homepage VARCHAR(100),
  fta_verb_id VARCHAR(100),
  fta_verb_display VARCHAR(100),
  fta_object_id VARCHAR(2000),
  fta_object_type VARCHAR(100),
  fta_object_definition_type VARCHAR(100),
  fta_object_definition_name VARCHAR(100),
  fta_context_category VARCHAR(100),
  fta_ip_address VARCHAR(100),
  fta_outside_of_school boolean,
  fta_event_type VARCHAR(100),
  fta_prev_event_type VARCHAR(100),
  fta_next_event_type VARCHAR(100),
  fta_date_dw_id BIGINT,
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  teacher_uuid VARCHAR(36),
  fta_start_time TIMESTAMP,
  fta_end_time TIMESTAMP,
  fta_timestamp_local VARCHAR(100),
  fta_time_spent DOUBLE PRECISION
);

CREATE TABLE alefdw.fact_teacher_activities
(
  fta_dw_id BIGINT IDENTITY(1,1),
  fta_created_time TIMESTAMP,
  fta_dw_created_time TIMESTAMP,
  fta_actor_object_type VARCHAR(100),
  fta_actor_account_homepage VARCHAR(100),
  fta_verb_id VARCHAR(100),
  fta_verb_display VARCHAR(100),
  fta_object_id VARCHAR(2000),
  fta_object_type VARCHAR(100),
  fta_object_definition_type VARCHAR(100),
  fta_object_definition_name VARCHAR(100),
  fta_context_category VARCHAR(100),
  fta_ip_address VARCHAR(100),
  fta_outside_of_school boolean,
  fta_event_type VARCHAR(100),
  fta_prev_event_type VARCHAR(100),
  fta_next_event_type VARCHAR(100),
  fta_date_dw_id BIGINT,
  fta_tenant_dw_id BIGINT,
  fta_school_dw_id BIGINT,
  fta_grade_dw_id BIGINT,
  fta_class_dw_id BIGINT,
  fta_subject_dw_id BIGINT,
  fta_teacher_dw_id BIGINT,
  fta_start_time TIMESTAMP,
  fta_end_time TIMESTAMP,
  fta_timestamp_local VARCHAR(100),
  fta_time_spent DOUBLE PRECISION
)
sortkey(fta_tenant_dw_id, fta_school_dw_id, fta_grade_dw_id, fta_class_dw_id, fta_teacher_dw_id);

CREATE TABLE alefdw_stage.staging_student_activities
(
  fsta_staging_id BIGINT IDENTITY(1,1),
  fsta_created_time TIMESTAMP,
  fsta_dw_created_time TIMESTAMP,
  fsta_actor_object_type VARCHAR(100),
  fsta_actor_account_homepage VARCHAR(100),
  fsta_verb_display VARCHAR(100),
  fsta_verb_id VARCHAR(100),
  fsta_object_id VARCHAR(2000),
  fsta_object_type VARCHAR(100),
  fsta_object_definition_type VARCHAR(100),
  fsta_object_definition_name VARCHAR(2000),
  fsta_from_time DECIMAL(5,0),
  fsta_to_time DECIMAL(5,0),
  fsta_ip_address VARCHAR(100),
  fsta_outside_of_school BOOLEAN,
  fsta_event_type VARCHAR(100),
  fsta_prev_event_type VARCHAR(100),
  fsta_next_event_type VARCHAR(100),
  fsta_date_dw_id BIGINT,
  fsta_attempt SMALLINT,
  fsta_score_raw DECIMAL(6,2),
  fsta_score_scaled DECIMAL(6,2),
  fsta_score_max DECIMAL(6,2),
  fsta_score_min DECIMAL(6,2),
  fsta_lesson_position SMALLINT,
  fsta_exp_id VARCHAR(36),
  fsta_ls_id VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  academic_year_uuid VARCHAR(36),
  fsta_timestamp_local VARCHAR (100),
  fsta_start_time TIMESTAMP,
  fsta_end_time TIMESTAMP,
  fsta_time_spent DOUBLE PRECISION
);

CREATE TABLE alefdw.fact_student_activities
(
  fsta_dw_id BIGINT IDENTITY(1,1),
  fsta_created_time TIMESTAMP,
  fsta_dw_created_time TIMESTAMP,
  fsta_actor_object_type VARCHAR(100),
  fsta_actor_account_homepage VARCHAR(100),
  fsta_verb_display VARCHAR(100),
  fsta_verb_id VARCHAR(100),
  fsta_object_id VARCHAR(2000),
  fsta_object_type VARCHAR(100),
  fsta_object_definition_type VARCHAR(100),
  fsta_object_definition_name VARCHAR(2000),
  fsta_from_time DECIMAL(5,0),
  fsta_to_time DECIMAL(5,0),
  fsta_ip_address VARCHAR(100),
  fsta_outside_of_school BOOLEAN,
  fsta_event_type VARCHAR(100),
  fsta_prev_event_type VARCHAR(100),
  fsta_next_event_type VARCHAR(100),
  fsta_date_dw_id BIGINT,
  fsta_attempt SMALLINT,
  fsta_score_raw DECIMAL(6,2),
  fsta_score_scaled DECIMAL(6,2),
  fsta_score_max DECIMAL(6,2),
  fsta_score_min DECIMAL(6,2),
  fsta_lesson_position SMALLINT,
  fsta_exp_id VARCHAR(36),
  fsta_ls_id VARCHAR(36),
  fsta_tenant_dw_id BIGINT,
  fsta_school_dw_id BIGINT,
  fsta_grade_dw_id BIGINT,
  fsta_class_dw_id BIGINT,
  fsta_subject_dw_id BIGINT,
  fsta_student_dw_id BIGINT,
  fsta_academic_year_dw_id BIGINT,
  fsta_timestamp_local VARCHAR (100),
  fsta_start_time TIMESTAMP,
  fsta_end_time TIMESTAMP,
  fsta_time_spent DOUBLE PRECISION
)
sortkey(fsta_tenant_dw_id, fsta_school_dw_id, fsta_grade_dw_id, fsta_class_dw_id, fsta_student_dw_id, fsta_academic_year_dw_id);

CREATE TABLE alefdw_stage.staging_guardian_app_activities
(
  fgaa_staging_id BIGINT IDENTITY(1,1),
  fgaa_created_time TIMESTAMP,
  fgaa_actor_object_type VARCHAR(100),
  fgaa_actor_account_homepage VARCHAR(100),
  fgaa_verb_display VARCHAR(100),
  fgaa_verb_id VARCHAR(100),
  fgaa_object_id VARCHAR(2000),
  fgaa_object_type VARCHAR(100),
  fgaa_object_account_homepage VARCHAR(100),
  fgaa_dw_created_time TIMESTAMP,
  fgaa_event_type VARCHAR(100),
  fgaa_date_dw_id BIGINT,
  fgaa_device VARCHAR(100),
  tenant_uuid VARCHAR(36),
  guardian_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  fgaa_timestamp_local VARCHAR(100)
);

CREATE TABLE alefdw.fact_guardian_app_activities
( fgaa_dw_id BIGINT IDENTITY(1,1),
  fgaa_created_time TIMESTAMP,
  fgaa_actor_object_type VARCHAR(100),
  fgaa_actor_account_homepage VARCHAR(100),
  fgaa_verb_display VARCHAR(100),
  fgaa_verb_id VARCHAR(100),
  fgaa_object_id VARCHAR(2000),
  fgaa_object_type VARCHAR(100),
  fgaa_object_account_homepage VARCHAR(100),
  fgaa_dw_created_time TIMESTAMP,
  fgaa_event_type VARCHAR(100),
  fgaa_date_dw_id BIGINT,
  fgaa_device VARCHAR(100),
  fgaa_tenant_dw_id BIGINT,
  fgaa_guardian_dw_id BIGINT,
  fgaa_student_dw_id BIGINT,
  fgaa_school_dw_id BIGINT,
  fgaa_timestamp_local VARCHAR(100)
)
sortkey(fgaa_guardian_dw_id);

CREATE TABLE alefdw_stage.staging_content_session
(
    fcs_staging_id            BIGINT IDENTITY (1,1),
    fcs_created_time          TIMESTAMP,
    fcs_dw_created_time       TIMESTAMP,
    fcs_date_dw_id            BIGINT,
    fcs_id                    VARCHAR(36),
    fcs_event_type            SMALLINT,
    fcs_is_start              BOOLEAN,
    fcs_ls_id                 VARCHAR(36),
    fcs_content_id            VARCHAR(36),
    fcs_lo_id                 VARCHAR(36),
    fcs_student_id            VARCHAR(36),
    fcs_class_id              VARCHAR(36),
    fcs_grade_id              VARCHAR(36),
    fcs_tenant_id             VARCHAR(36),
    fcs_school_id             VARCHAR(36),
    fcs_ay_id                 VARCHAR(36),
    fcs_section_id            VARCHAR(36),
    fcs_lp_id                 VARCHAR(36),
    fcs_ip_id                 VARCHAR(36),
    fcs_outside_of_school     BOOLEAN,
    fcs_content_academic_year VARCHAR(4),
    fcs_app_timespent         DOUBLE PRECISION,
    fcs_app_score             DOUBLE PRECISION
);

CREATE TABLE alefdw.fact_content_session
(
    fcs_dw_id                 BIGINT IDENTITY (1,1),
    fcs_created_time          TIMESTAMP,
    fcs_dw_created_time       TIMESTAMP,
    fcs_date_dw_id            BIGINT,
    fcs_id                    VARCHAR(36),
    fcs_event_type            SMALLINT,
    fcs_is_start              BOOLEAN,
    fcs_ls_id                 VARCHAR(36),
    fcs_content_id            VARCHAR(36),
    fcs_lo_dw_id              BIGINT,
    fcs_student_dw_id         BIGINT,
    fcs_class_dw_id           BIGINT,
    fcs_grade_dw_id           BIGINT,
    fcs_tenant_dw_id          BIGINT,
    fcs_school_dw_id          BIGINT,
    fcs_ay_dw_id              BIGINT,
    fcs_section_dw_id         BIGINT,
    fcs_lp_dw_id              BIGINT,
    fcs_ip_id                 VARCHAR(36),
    fcs_outside_of_school     BOOLEAN,
    fcs_content_academic_year VARCHAR(4),
    fcs_app_timespent         DOUBLE PRECISION,
    fcs_app_score             DOUBLE PRECISION
)
sortkey (fcs_school_dw_id, fcs_grade_dw_id, fcs_class_dw_id, fcs_student_dw_id, fcs_date_dw_id);

---------------STATIC DIMENSIONS-----------------------------------
CREATE TABLE alefdw.dim_tenant
(
tenant_dw_id BIGINT IDENTITY(1,1),
tenant_id VARCHAR(36),
tenant_name VARCHAR(765),
tenant_timezone VARCHAR(100)
)
diststyle all
compound sortkey(tenant_dw_id);

INSERT INTO alefdw.dim_tenant(
  tenant_id,
  tenant_name,
  tenant_timezone
) VALUES
 ('21ce4d98-8286-4f7e-b122-03b98a5f3b2e', 'Private','Asia/Dubai'),
 ('93e4949d-7eff-4707-9201-dac917a5e013', 'MOE','Asia/Dubai'),
 ('2746328b-4109-4434-8517-940d636ffa09', 'HCZ','EST');

CREATE TABLE alefdw.dim_curriculum_subject
(
  curr_subject_dw_id BIGINT IDENTITY(1,1),
  curr_subject_id VARCHAR(36),
  curr_subject_name VARCHAR(255)

)
diststyle all
compound sortkey(curr_subject_dw_id);

INSERT INTO alefdw.dim_curriculum_subject(
  curr_subject_id,
  curr_subject_name
) VALUES
 ('571671', 'Math'),
 ('742980', 'Science'),
 ('186926', 'English'),
 ('658224', 'Social_Studies'),
 ('352071', 'Arabic'),
 ('934714', 'Islamic_Studies'),
 ('876678', 'English_Access');

CREATE TABLE alefdw.dim_curriculum_grade
(
  curr_grade_dw_id BIGINT IDENTITY(1,1),
  curr_grade_id VARCHAR(36),
  curr_grade_name VARCHAR(20)
)
diststyle all
compound sortkey(curr_grade_dw_id);

INSERT INTO alefdw.dim_curriculum_grade(
  curr_grade_id,
  curr_grade_name
) VALUES
 ('333938', '1'),
 ('128785', '2'),
 ('439646', '3'),
 ('363684', '4'),
 ('231997', '5'),
 ('322135', '6'),
 ('596550', '7'),
 ('768780', '8'),
 ('675029', '9'),
 ('865858', '10'),
 ('104322', '11'),
 ('566271', '12');

CREATE TABLE alefdw.dim_curriculum
(
  curr_dw_id BIGINT IDENTITY(1,1),
  curr_id VARCHAR(36),
  curr_name VARCHAR(255),
  curr_academic_period_order SMALLINT,
  curr_academic_period_start_date TIMESTAMP,
  curr_academic_period_end_date TIMESTAMP
)
diststyle all
compound sortkey(curr_dw_id);

INSERT INTO alefdw.dim_curriculum(
  curr_id ,
  curr_name,
  curr_academic_period_order ,
  curr_academic_period_start_date ,
  curr_academic_period_end_date
) VALUES
 ('392027', 'UAE MOE',1,'2018-08-01 00:00:00.000','2018-12-16 23:59:59.000'),
 ('392027', 'UAE MOE',2,'2019-01-06 00:00:00.000','2019-03-31 23:59:59.000'),
 ('392027', 'UAE MOE',3,'2019-04-14 00:00:00.000','2019-07-04 23:59:59.000'),
 ('563622', 'NYDOE',1,'2018-08-01 00:00:00.000','2018-11-09 23:59:59.000'),
 ('563622', 'NYDOE',2,'2018-11-12 00:00:00.000','2019-02-01 23:59:59.000'),
 ('563622', 'NYDOE',3,'2019-02-04 00:00:00.000','2019-04-12 23:59:59.000'),
 ('563622', 'NYDOE',4,'2019-04-15 00:00:00.000','2019-06-26 23:59:59.000');

CREATE TABLE alefdw.dim_role
(
  role_dw_id BIGINT IDENTITY(1,1),
  role_id VARCHAR(50)
)
diststyle all
compound sortkey(role_dw_id);

INSERT INTO alefdw.dim_role(
  role_id
) VALUES
 ('TEACHER'),
 ('STUDENT'),
 ('GUARDIAN'),
 ('TDC'),
 ('PRINCIPAL');

CREATE TABLE alefdw.dim_award_category
(
  award_category_dw_id BIGINT IDENTITY(1,1),
  award_category_id VARCHAR(20),
  award_category_level_ar VARCHAR(100),
  award_category_level_en VARCHAR(100)
)
sortkey(award_category_dw_id);

INSERT INTO alefdw.dim_award_category(
  award_category_id,
  award_category_level_en,
  award_category_level_ar
  ) VALUES
  ('gim', 'Good improvement','تحـســن ملحــوظ'),
  ('gcp', 'Good class participation', 'مشاركة جـيـــدة في الحـصــة'),
  ('gms', 'Good mindset', 'قدرات عقلية جيدة'),
  ('gqn', 'Good Questions', 'قدرات عقلية جيدة'),
  ('gat', 'Good attendance', 'حـضــور متـمـيــز'),
  ('gel', 'Good experiential lesson', 'أداء جيد في الـدرس التجريـبي'),
  ('gor', 'Good organization', 'مـنـظـــــم ومرتـب'),
  ('gtm', 'Good time management', 'حســن الاستـفـادة من الوقــت'),
  ('gbh', 'Good Behavior', 'سلوك وخلق جيدين');

------------------------------------------------------------------------------

create table if not exists alefdw.dim_date
(
        date_id int not null unique,
        full_date date not null,
        weekday_name_short VARCHAR(5) not null,
        weekday_name_long VARCHAR(20) not null,
        day_of_week int not null,
        is_weekend boolean not null,
        calendar_week_of date not null,
        calendar_week_number int not null,
        calendar_month_name_short VARCHAR(20) not null,
        calendar_month_name_long VARCHAR(20) not null,
        calendar_month_number integer not null,
        calendar_month_start_date date not null,
        calendar_month_end_date date not null,
        calendar_quarter_name VARCHAR(20) not null,
        calendar_quarter_number integer not null,
        calendar_quarter_start_date date not null,
        calendar_quarter_end_date date not null,
        calendar_year_week_name VARCHAR(20) not null,
        calendar_year_week_number integer not null,
        calendar_year_month_name VARCHAR(20) not null,
        calendar_year_month_number integer not null,
        calendar_year_quarter_name VARCHAR(20) not null,
        calendar_year_quarter_number integer not null,
        calendar_year_number integer not null,
        calendar_year_name VARCHAR(20) not null,
        calendar_year_start_date date not null,
        calendar_year_end_date date not null,
        broadcast_week_number integer not null,
        broadcast_week_start_date date not null,
        broadcast_week_end_date date not null,
        broadcast_year_week_name VARCHAR(20) not null,
        broadcast_year_week_number integer not null,
        broadcast_month_start_date date not null,
        broadcast_month_end_date date not null,
        broadcast_month_number VARCHAR(20) not null,
        broadcast_month_name_long VARCHAR(20) not null,
        broadcast_month_name_short VARCHAR(20) not null,
        broadcast_year_month_name VARCHAR(20) not null,
        broadcast_year_month_number integer not null,
        broadcast_quarter_start_date date not null,
        broadcast_quarter_end_date date not null,
        broadcast_quarter_number int not null,
        broadcast_quarter_name VARCHAR(20) not null,
        broadcast_year_quarter_name VARCHAR(20) not null,
        broadcast_year_quarter_number integer not null,
        broadcast_year_start_date date not null,
        broadcast_year_end_date date not null,
        broadcast_year_number int not null,
        primary key(date_id)
)
diststyle all
sortkey(date_id, full_date);

create table alefdw.dim_event_type(
  event_type_dw_id BIGINT IDENTITY(1,1),
  event_type VARCHAR(100),
  event_type_tag VARCHAR(50)
);


INSERT INTO alefdw.dim_event_type(event_type, event_type_tag)
VALUES
('teacher.platform.login','Teacher Others'),
('teacher.platform.logout','Teacher Others'),
('teacher.homePage.accessed,,','Teacher Others'),
('teacher.lessonsPage.accessed','Teacher Others'),
('teacher.lessonsPage.lesson.interacted','Teacher Others'),
('teacher.lessons.previewed','Teacher Lesson Preview'),
('teacher.learnPage.content.initialized ','Teacher Lesson Preview'),
('teacher.learnPage.content.completed','Teacher Lesson Preview'),
('teacher.learnPage.content.played','Teacher Lesson Preview'),
('teacher.learnPage.content.paused','Teacher Lesson Preview'),
('teacher.learnPage.content.resumed','Teacher Lesson Preview'),
('teacher.learnPage.content.started','Teacher Lesson Preview'),
('teacher.lessonsPage.assigned','Teacher Others'),
('teacher.lessonsPage.cancelled','Teacher Others'),
('teacher.lessonsPage.unlocked','Teacher Others'),
('teacher.lessonsPage.locked','Teacher Others'),
('teacher.lessonsPage.progressControlSet','Teacher Others'),
('teacher.lessonsPage.progressControlCancelled','Teacher Others'),
('teacher.schedulePage.accessed','Teacher Others'),
('teacher.classPerformance.accessed','Teacher Analytics'),
('teacher.grade.selected','Teacher Analytics'),
('teacher.class.selected','Teacher Analytics'),
('teacher.subject.selected','Teacher Analytics'),
('teacher.group.selected','Teacher Analytics'),
('teacher.study.period.selected','Teacher Analytics'),
('teacher.classPerformance.column.sorted','Teacher Analytics'),
('teacher.classPerformance.cell.focused','Teacher Analytics'),
('teacher.lessonReportPage.lesson.interacted','Teacher Analytics'),
('teacher.lessonReportPage.accessed','Teacher Analytics'),
('teacher.student.interacted','Teacher Analytics'),
('teacher.studentReportPage.accessed','Teacher Analytics'),
('teacher.lesson.report.page.expanded','Teacher Analytics'),
('teacher.lesson.report.page.collapsed','Teacher Analytics'),
('teacher.performance.item.focused','Teacher Analytics'),
('teacher.performance.item.closed','Teacher Analytics'),
('teacher.lessonsReportPage.lesson.selected','Teacher Analytics'),
('teacher.lessonReportPage.assigned','Teacher Analytics'),
('teacher.lessonReportPage.cancelled','Teacher Analytics'),
('teacher.lessonReportPage.unlocked','Teacher Analytics'),
('teacher.lessonReportPage.locked','Teacher Analytics'),
('teacher.lessonReportPage.progressControlSet','Teacher Analytics'),
('teacher.lessonReportPage.progressControlCancelled','Teacher Analytics'),
('teacher.student.report.page.expanded','Teacher Analytics'),
('teacher.student.report.page.collapsed','Teacher Analytics'),
('teacher.performance.item.focused','Teacher Analytics'),
('teacher.performance.item.closed','Teacher Analytics'),
('teacher.studentReportPage.student.selected','Teacher Analytics');

INSERT INTO alefdw.dim_event_type(event_type, event_type_tag)
VALUES
('student.learningPathPage.lesson.launched','Student lesson page'),
('student.learningPathPage.lesson.resumed','Student lesson page'),
('student.learningPathPage.lesson.reviewed','Student lesson page'),
('student.learningPathPage.lesson.reattempted','Student lesson page'),
('student.mloLearnPage.content.initialized', 'Student MLO'),
('student.mloLearnPage.content.completed','Student MLO'),
('student.mloLearnPage.content.played','Student MLO'),
('student.mloLearnPage.content.played','Student video'),
('student.mloLearnPage.content.paused','Student MLO'),
('student.mloLearnPage.content.paused','Student video'),
('student.mloLearnPage.content.resumed','Student MLO'),
('student.mloLearnPage.content.resumed','Student video'),
('student.mloLearnPage.content.seeked','Student MLO'),
('student.mloLearnPage.content.seeked','Student video'),
('student.mloLearnPage.content.completed','Student MLO'),
('student.mloLearnPage.content.completed','Student video'),
('student.mloLearnPage.lesson.reviewed','Student MLO'),
('student.mloLearnPage.content.accessed','Student MLO'),
('student.mloLearnPage.content.accessed','Student Background Knowledge'),
('student.mloLearnPage.content.accessed','Student MLO'),
('student.mloLearnPage.content.accessed','Student KT library');

INSERT INTO alefdw.dim_event_type(event_type, event_type_tag)
VALUES
('guardian.app.activity.accessed','Guardian App Activity'),
('guardian.app.student.selected','Guardian App Activity')

INSERT INTO alefdw.dim_class(class_created_time, class_updated_time, class_deleted_time, class_dw_created_time, class_dw_updated_time, class_status, class_id, class_name, class_section, class_enabled)
VALUES (getdate(), getdate(), null, getdate(), getdate(), 1, 'DEFAULT_ID', 'DEFAULT_CLASS', 'DEFAULT_CLASS', true );

INSERT INTO alefdw.dim_subject(subject_created_time, subject_updated_time, subject_deleted_time, subject_dw_created_time, subject_dw_updated_time, subject_status, subject_id, subject_name, subject_online, subject_gen_subject)
VALUES (getdate(), getdate(), null, getdate(), getdate(), 1, 'DEFAULT_ID', 'DEFAULT_SUBJECT', true , '' );

INSERT INTO alefdw.dim_grade(grade_created_time, grade_updated_time, grade_deleted_time, grade_dw_created_time, grade_dw_updated_time, grade_status, grade_id, grade_name, grade_k12grade)
VALUES (getdate(), getdate(), null, getdate(), getdate(), 1, 'DEFAULT_ID', 'DEFAULT_GRADE', -1);

INSERT INTO alefdw.dim_school(school_created_time, school_updated_time, school_deleted_time, school_dw_created_time, school_dw_updated_time, school_status, school_id, school_name, school_organisation, school_address_line, school_post_box, school_city_name, school_country_name, school_latitude, school_longitude, school_first_day, school_timezone, school_composition)
VALUES (getdate(), getdate(), null, getdate(), getdate(), 1, 'DEFAULT_ID', 'DEFAULT_SCHOOL', 'DEFAULT', 'DEFAULT', '', 'DEFAULT', 'DEFAULT', null, null, 'N/A', 'N/A', 'N/A');

------------------------------------------------------------------------------------------------------------------------

alter table alefdw_stage.rel_teacher
    add column school_uuid VARCHAR(36);

alter table alefdw.dim_teacher
    add column teacher_school_dw_id bigint;

alter table alefdw_stage.staging_practice_session drop column practice_session_item_skill_uuid;
alter table alefdw_stage.staging_practice_session drop column skill_uuid;

alter table alefdw.fact_practice_session drop column practice_session_skill_dw_id;
alter table alefdw.fact_practice_session drop column practice_session_item_skill_dw_id;

alter table alefdw_stage.staging_practice_session add column practice_session_outside_of_school BOOLEAN;
alter table alefdw.fact_practice_session add column practice_session_outside_of_school BOOLEAN;

alter table alefdw_stage.staging_practice_session add column practice_session_stars INT;
alter table alefdw.fact_practice_session add column practice_session_stars INT;

alter table alefdw.dim_tdc drop column tdc_avatar;
alter table alefdw_stage.rel_tdc drop column tdc_avatar;

alter table alefdw.dim_principal drop column principal_avatar;
alter table alefdw_stage.rel_principal drop column principal_avatar;


create table alefdw_stage.staging_ktg
(
  ktg_staging_id BIGINT IDENTITY(1,1),
  ktg_id VARCHAR(256),
  ktg_created_time TIMESTAMP,
  ktg_dw_created_time TIMESTAMP,
  ktg_date_dw_id BIGINT,
  tenant_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  academic_year_uuid VARCHAR(36),
  ktg_num_key_terms SMALLINT,
  ktg_kt_collection_id BIGINT,
  ktg_trimester_id VARCHAR(256),
  ktg_trimester_order SMALLINT,
  ktg_type VARCHAR(200),
  ktg_question_type VARCHAR(200),
  ktg_min_question SMALLINT,
  ktg_max_question SMALLINT,
  ktg_question_time_allotted INT
);

create table alefdw.fact_ktg
(
  ktg_dw_id BIGINT IDENTITY(1,1),
  ktg_id VARCHAR(256),
  ktg_created_time TIMESTAMP,
  ktg_dw_created_time TIMESTAMP,
  ktg_date_dw_id BIGINT,
  ktg_tenant_dw_id BIGINT,
  ktg_student_dw_id BIGINT,
  ktg_subject_dw_id BIGINT,
  ktg_school_dw_id BIGINT,
  ktg_grade_dw_id BIGINT,
  ktg_class_dw_id BIGINT,
  ktg_lo_dw_id BIGINT,
  ktg_academic_year_dw_id BIGINT,
  ktg_num_key_terms SMALLINT,
  ktg_kt_collection_id BIGINT,
  ktg_trimester_id VARCHAR(256),
  ktg_trimester_order SMALLINT,
  ktg_type VARCHAR(200),
  ktg_question_type VARCHAR(200),
  ktg_min_question SMALLINT,
  ktg_max_question SMALLINT,
  ktg_question_time_allotted INT
)
sortkey(ktg_dw_id, ktg_student_dw_id, ktg_subject_dw_id, ktg_school_dw_id, ktg_grade_dw_id, ktg_class_dw_id, ktg_lo_dw_id, ktg_academic_year_dw_id);

create table alefdw_stage.staging_ktg_session
(
  ktg_session_staging_id BIGINT IDENTITY(1,1),
  ktg_session_id VARCHAR(256),
  ktg_session_created_time TIMESTAMP,
  ktg_session_dw_created_time TIMESTAMP,
  ktg_session_date_dw_id BIGINT,
  ktg_session_question_id VARCHAR(256),
  ktg_session_kt_id VARCHAR(256),
  tenant_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  academic_year_uuid VARCHAR(36),
  ktg_session_outside_of_school BOOLEAN,
  ktg_session_trimester_id VARCHAR(256),
  ktg_session_trimester_order SMALLINT,
  ktg_session_type VARCHAR(200),
  ktg_session_question_time_allotted INT,
  ktg_session_answer VARCHAR(5000), -- all the answers (attempts) in case of memory game, one answer for rocket game
  ktg_session_num_attempts SMALLINT, -- null for rocket game, number of attempts to right answer for memory game
  ktg_session_score DOUBLE PRECISION,
  ktg_session_max_score DOUBLE PRECISION,
  ktg_session_stars INT,
  ktg_session_is_attended BOOLEAN,
  ktg_session_event_type INT, -- 1 for GameSession and 2 for GameQuestionSession
  ktg_session_is_start BOOLEAN,
  ktg_session_is_start_event_processed BOOLEAN
);

create table alefdw.fact_ktg_session
(
  ktg_session_dw_id BIGINT IDENTITY(1,1),
  ktg_session_id VARCHAR(256),
  ktg_session_start_time TIMESTAMP,
  ktg_session_end_time TIMESTAMP,
  ktg_session_dw_created_time TIMESTAMP,
  ktg_session_date_dw_id BIGINT,
  ktg_session_question_id VARCHAR(256),
  ktg_session_kt_id VARCHAR(256),
  ktg_session_tenant_dw_id BIGINT,
  ktg_session_student_dw_id BIGINT,
  ktg_session_subject_dw_id BIGINT,
  ktg_session_school_dw_id BIGINT,
  ktg_session_grade_dw_id BIGINT,
  ktg_session_class_dw_id BIGINT,
  ktg_session_lo_dw_id BIGINT,
  ktg_session_academic_year_dw_id BIGINT,
  ktg_session_outside_of_school BOOLEAN,
  ktg_session_trimester_id VARCHAR(256),
  ktg_session_trimester_order SMALLINT,
  ktg_session_type VARCHAR(200),
  ktg_session_question_time_allotted INT,
  ktg_session_time_spent INT,
  ktg_session_answer VARCHAR(5000),
  ktg_session_num_attempts SMALLINT,
  ktg_session_score DOUBLE PRECISION,
  ktg_session_max_score DOUBLE PRECISION,
  ktg_session_stars INT,
  ktg_session_is_attended BOOLEAN,
  ktg_session_event_type INT,
  ktg_session_is_start BOOLEAN
)
sortkey(ktg_session_dw_id, ktg_session_student_dw_id, ktg_session_subject_dw_id, ktg_session_school_dw_id, ktg_session_grade_dw_id, ktg_session_class_dw_id, ktg_session_lo_dw_id, ktg_session_academic_year_dw_id);

create table alefdw_stage.staging_ktgskipped
(
  ktgskipped_staging_id BIGINT IDENTITY(1,1),
  ktgskipped_created_time TIMESTAMP,
  ktgskipped_dw_created_time TIMESTAMP,
  ktgskipped_date_dw_id BIGINT,
  tenant_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  academic_year_uuid VARCHAR(36),
  ktgskipped_num_key_terms SMALLINT,
  ktgskipped_kt_collection_id BIGINT,
  ktgskipped_trimester_id VARCHAR(256),
  ktgskipped_trimester_order SMALLINT,
  ktgskipped_type VARCHAR(200),
  ktgskipped_min_question SMALLINT,
  ktgskipped_max_question SMALLINT,
  ktgskipped_question_type VARCHAR(1000),
  ktgskipped_question_time_allotted INT
);

create table alefdw.fact_ktgskipped
(
  ktgskipped_dw_id BIGINT IDENTITY(1,1),
  ktgskipped_created_time TIMESTAMP,
  ktgskipped_dw_created_time TIMESTAMP,
  ktgskipped_date_dw_id BIGINT,
  ktgskipped_tenant_dw_id BIGINT,
  ktgskipped_student_dw_id BIGINT,
  ktgskipped_subject_dw_id BIGINT,
  ktgskipped_school_dw_id BIGINT,
  ktgskipped_grade_dw_id BIGINT,
  ktgskipped_class_dw_id BIGINT,
  ktgskipped_lo_dw_id BIGINT,
  ktgskipped_academic_year_dw_id BIGINT,
  ktgskipped_num_key_terms SMALLINT,
  ktgskipped_kt_collection_id BIGINT,
  ktgskipped_trimester_id VARCHAR(256),
  ktgskipped_trimester_order SMALLINT,
  ktgskipped_min_question SMALLINT,
  ktgskipped_max_question SMALLINT,
  ktgskipped_type VARCHAR(200),
  ktgskipped_question_type VARCHAR(1000),
  ktgskipped_question_time_allotted INT
)
sortkey(ktgskipped_dw_id, ktgskipped_student_dw_id, ktgskipped_subject_dw_id, ktgskipped_school_dw_id, ktgskipped_grade_dw_id, ktgskipped_class_dw_id, ktgskipped_lo_dw_id, ktgskipped_academic_year_dw_id);

alter table alefdw.fact_practice
  add column practice_academic_year_dw_id BIGINT;

alter table alefdw.fact_practice_session
  add column practice_academic_year_dw_id BIGINT;

alter table alefdw_stage.staging_practice
  add column academic_year_uuid VARCHAR(256);

alter table alefdw_stage.staging_practice_session
  add column academic_year_uuid VARCHAR(256);

alter table alefdw_stage.staging_learning_experience
  add column academic_year_uuid VARCHAR(256);

alter table alefdw_stage.staging_learning_experience
  add column fle_content_academic_year VARCHAR(20);

alter table alefdw.fact_learning_experience
  add column fle_academic_year_dw_id BIGINT;

alter table alefdw.fact_learning_experience
  add column fle_content_academic_year VARCHAR(20);

alter table alefdw_stage.staging_star_awarded
  add column academic_year_uuid VARCHAR(256);

alter table alefdw.fact_star_awarded
  add column fsa_academic_year_dw_id BIGINT;

alter table alefdw.dim_school
  add column school_tenant_id varchar(36);
------This has been applied as Data 6.0 Release on 31st July--------------------------

alter table alefdw.dim_school
  add column school_alias varchar(256);

------This has been applied on Aug 18 as partial Data 6.1 Release --------------------

-- DDL for in-class-game ALEF-9568
-- alefdw_stage

create table if not exists alefdw_stage.staging_inc_game (
      inc_game_staging_id BIGINT IDENTITY(1,1),
      inc_game_id VARCHAR(36),
      inc_game_event_type INT,
      inc_game_created_time TIMESTAMP,
      inc_game_dw_created_time TIMESTAMP,
      inc_game_date_dw_id BIGINT,
      tenant_uuid VARCHAR(36),
      school_uuid VARCHAR(36),
      class_uuid VARCHAR(36),
      lo_uuid VARCHAR(36),
      inc_game_title VARCHAR(256),
      teacher_uuid VARCHAR(36),
      subject_uuid VARCHAR(36),
      grade_uuid VARCHAR(36),
      learning_path_uuid VARCHAR(36),
      inc_game_num_questions INT
);

create table if not exists alefdw_stage.staging_inc_game_session (
      inc_game_session_staging_id BIGINT IDENTITY(1,1),
      inc_game_session_id VARCHAR(36),
      inc_game_session_created_time TIMESTAMP,
      inc_game_session_dw_created_time TIMESTAMP,
      inc_game_session_date_dw_id BIGINT,
      tenant_uuid VARCHAR(36),
      game_uuid VARCHAR(36),
      inc_game_session_title VARCHAR(256),
      inc_game_session_num_players INT,
      inc_game_session_num_joined_players INT,
      inc_game_session_started_by VARCHAR(36),
      inc_game_session_status INT,  -- 0 - NOT_STARTED - 1 - IN_PROGRESS, 2 - COMPLETED, 3 - CANCELLED
      inc_game_session_is_start BOOLEAN default false,
      inc_game_session_is_start_event_processed BOOLEAN default false
);

create table if not exists alefdw_stage.staging_inc_game_outcome (
      inc_game_outcome_staging_id BIGINT IDENTITY(1,1),
      inc_game_outcome_id VARCHAR(36),
      inc_game_outcome_created_time TIMESTAMP,
      inc_game_outcome_dw_created_time TIMESTAMP,
      inc_game_outcome_date_dw_id BIGINT,
      tenant_uuid VARCHAR(36),
      session_uuid VARCHAR(36),
      game_uuid VARCHAR(36),
      player_uuid VARCHAR(36),
      lo_uuid VARCHAR(36),
      inc_game_outcome_score DOUBLE PRECISION,
      inc_game_outcome_status INT
);

-- alefdw

create table if not exists alefdw.fact_inc_game (
      inc_game_dw_id BIGINT IDENTITY(1,1),
      inc_game_id VARCHAR(36),
      inc_game_created_time TIMESTAMP,
      inc_game_dw_created_time TIMESTAMP,
      inc_game_date_dw_id BIGINT,
      inc_game_updated_time TIMESTAMP,
      inc_game_dw_updated_time TIMESTAMP,
      inc_game_tenant_dw_id BIGINT,
      inc_game_school_dw_id BIGINT,
      inc_game_class_dw_id BIGINT,
      inc_game_lo_dw_id BIGINT,
      inc_game_title VARCHAR(256),
      inc_game_teacher_dw_id BIGINT,
      inc_game_subject_dw_id BIGINT,
      inc_game_grade_dw_id BIGINT,
      inc_game_learning_path_dw_id BIGINT,
      inc_game_num_questions INT
);

create table if not exists alefdw.fact_inc_game_session (
      inc_game_session_dw_id BIGINT IDENTITY(1,1),
      inc_game_session_id VARCHAR(36),
      inc_game_session_start_time TIMESTAMP,
      inc_game_session_end_time TIMESTAMP,
      inc_game_session_dw_created_time TIMESTAMP,
      inc_game_session_date_dw_id BIGINT,
      inc_game_session_time_spent INT,
      inc_game_session_tenant_dw_id BIGINT,
      inc_game_session_game_id VARCHAR(36),
      inc_game_session_title VARCHAR(256),
      inc_game_session_num_players INT,
      inc_game_session_num_joined_players INT,
      inc_game_session_started_by_dw_id BIGINT,
      inc_game_session_status INT, -- 0 - UNDEFINED - 1 - IN_PROGRESS, 2 - COMPLETED, 3 - CANCELLED
      inc_game_session_is_start BOOLEAN
);

create table if not exists alefdw.fact_inc_game_outcome (
      inc_game_outcome_dw_id BIGINT IDENTITY(1,1),
      inc_game_outcome_id VARCHAR(36),
      inc_game_outcome_created_time TIMESTAMP,
      inc_game_outcome_dw_created_time TIMESTAMP,
      inc_game_outcome_date_dw_id BIGINT,
      inc_game_outcome_tenant_dw_id BIGINT,
      inc_game_outcome_session_id VARCHAR(36),
      inc_game_outcome_game_id VARCHAR(36),
      inc_game_outcome_player_dw_id BIGINT,
      inc_game_outcome_lo_dw_id BIGINT,
      inc_game_outcome_score DOUBLE PRECISION,
      inc_game_outcome_status INT -- 0 - UNDEFINED, 1 - COMPLETED, 2 - SESSION_CANCELLED, 3 - SESSION_LEFT,
);


alter table alefdw.dim_grade add column academic_year_id varchar(36);
alter table alefdw.dim_curriculum add column curr_content_academic_year varchar(50);

create table if not exists alefdw_stage.rel_class_schedule (
    rel_class_schedule_id BIGINT IDENTITY (1,1),
    class_schedule_day_of_week VARCHAR(10),
    class_schedule_start_time VARCHAR(20),
    class_schedule_end_time VARCHAR(20),
    class_schedule_created_time TIMESTAMP,
    class_schedule_updated_time TIMESTAMP,
    class_schedule_deleted_time TIMESTAMP,
    class_schedule_dw_created_time TIMESTAMP,
    class_schedule_dw_updated_time TIMESTAMP,
    class_schedule_active_until TIMESTAMP,
    class_schedule_status INT,
    class_uuid VARCHAR(36),
    tenant_uuid VARCHAR(36),
    subject_uuid VARCHAR(36),
    teacher_uuid VARCHAR(36)
);

create table if not exists alefdw.dim_class_schedule (
    rel_class_schedule_dw_id BIGINT IDENTITY (1,1),
    class_id VARCHAR(36),
    class_schedule_day_of_week VARCHAR(10),
    class_schedule_start_time VARCHAR(20),
    class_schedule_end_time VARCHAR(20),
    class_schedule_created_time TIMESTAMP,
    class_schedule_updated_time TIMESTAMP,
    class_schedule_deleted_time TIMESTAMP,
    class_schedule_dw_created_time TIMESTAMP,
    class_schedule_dw_updated_time TIMESTAMP,
    class_schedule_active_until TIMESTAMP,
    class_schedule_status INT,
    class_schedule_class_dw_id BIGINT,
    class_schedule_tenant_dw_id BIGINT,
    class_schedule_subject_dw_id BIGINT,
    class_schedule_teacher_dw_id BIGINT
);

ALTER TABLE alefdw_stage.staging_conversation_occurred ADD COLUMN fco_learning_session_id VARCHAR(36);
ALTER TABLE alefdw.fact_conversation_occurred ADD COLUMN fco_learning_session_id VARCHAR(36);

alter table alefdw.fact_teacher_activities alter column fta_timestamp_local type varchar(600);
alter table alefdw.fact_student_activities alter column fsta_timestamp_local type varchar(600);
alter table alefdw.fact_guardian_app_activities alter column fgaa_timestamp_local type varchar(600);

alter table alefdw_stage.staging_teacher_activities alter column fta_timestamp_local type varchar(600);
alter table alefdw_stage.staging_student_activities alter column fsta_timestamp_local type varchar(600);
alter table alefdw_stage.staging_guardian_app_activities alter column fgaa_timestamp_local type varchar(600);


create table alefdw.curio_student(
    student_dw_id BIGINT,
    adek_id varchar(36),
    time_stamp timestamp,
    status varchar(20),
    interaction_type varchar(50),
    topic_name varchar(800),
    subject_id varchar(200),
    role varchar(20),
    date_dw_id BIGINT,
    loadtime timestamp
);

create table alefdw.curio_teacher(
    student_id varchar(36),
    teacher_id varchar(36),
    topic varchar(800),
    created_at timestamp,
    updated_at timestamp,
    date_dw_id BIGINT,
    loadtime timestamp
);


alter table alefdw.dim_class add column tenant_id VARCHAR(36);
alter table alefdw.dim_class add column grade_id VARCHAR(36);
alter table alefdw.dim_class add column school_id VARCHAR(36);

alter table alefdw.dim_grade add column tenant_id VARCHAR(36);
alter table alefdw.dim_grade add column school_id VARCHAR(36);

create table alefdw.curio_student(
    adek_id varchar(100),
    time_stamp timestamp,
    status varchar(20),
    interaction_type varchar(50),
    topic_name varchar(800),
    subject_id varchar(200),
    role varchar(20),
    date_dw_id BIGINT,
    loadtime timestamp
);

create table alefdw.curio_teacher(
    student_id varchar(100),
    teacher_id varchar(100),
    topic varchar(800),
    created_at timestamp,
    updated_at timestamp,
    date_dw_id BIGINT,
    loadtime timestamp
);

alter table alefdw.dim_learning_path add column learning_path_academic_year_id varchar(36);
alter table alefdw.dim_learning_path add column learning_path_content_academic_year int;

alter table alefdw_stage.staging_learning_experience add column fle_time_spent_app int;
alter table alefdw.fact_learning_experience add column fle_time_spent_app int;

------------NoToBeApplied------CCLChanges and Assignment Changes--------------------------------------------------------

create table alefdw.dim_category
(
    category_dw_id bigint BIGINT IDENTITY(1,1),
    category_created_time TIMESTAMP,
    category_updated_time TIMESTAMP,
    category_deleted_time TIMESTAMP,
    category_dw_created_time TIMESTAMP,
    category_dw_updated_time TIMESTAMP,
    category_status INTEGER,
    category_id VARCHAR(36),
    category_name VARCHAR(500),
    category_code VARCHAR(150),
    category_description VARCHAR(500)
)
diststyle all
sortkey(category_dw_id);

CREATE TABLE alefdw_stage.rel_category_association
(
  rel_category_association_id BIGINT IDENTITY(1,1),
  category_association_created_time TIMESTAMP,
  category_association_deleted_time TIMESTAMP,
  category_association_dw_created_time TIMESTAMP,
  category_association_active_until TIMESTAMP,
  category_association_status INT,
  category_association_category_uuid VARCHAR(36),
  category_association_uuid VARCHAR(36),
  category_association_type INT--1=strand,2=standard etc
)

CREATE TABLE alefdw.dim_category_association
(
  rel_category_association_dw_id BIGINT IDENTITY(1,1),
  category_association_created_time TIMESTAMP,
  category_association_deleted_time TIMESTAMP,
  category_association_dw_created_time TIMESTAMP,
  category_association_active_until TIMESTAMP,
  category_association_status INT,
  category_association_skill_dw_id BIGINT,
  category_association_dw_id BIGINT,
  category_association_type INT--1=strand,2=standard etc
)
diststyle all
sortkey(category_association_skill_dw_id);

create table alefdw.dim_strand
(
	strand_dw_id bigint default "identity"(206121, 0, '1,1'::text),
	strand_created_time timestamp,
	strand_updated_time timestamp,
	strand_deleted_time timestamp,
	strand_dw_created_time timestamp,
	strand_dw_updated_time timestamp,
	strand_status integer,
	strand_id bigint,
	strand_name varchar(500),
	strand_description varchar(500),
	strand_curriculum_id bigint,
	strand_grade_id bigint,
	strand_subject_id bigint
)
diststyle all
sortkey(strand_dw_id);

create table alefdw.dim_standard
(
	standard_dw_id bigint default "identity"(206121, 0, '1,1'::text),
	standard_created_time timestamp,
	standard_updated_time timestamp,
	standard_deleted_time timestamp,
	standard_dw_created_time timestamp,
	standard_dw_updated_time timestamp,
	standard_status integer,
	standard_id bigint,
	standard_name varchar(500),
	standard_description varchar(500),
	standard_strand_id bigint
)
diststyle all
sortkey(standard_dw_id);

create table alefdw.dim_substandard
(
	substandard_dw_id bigint default "identity"(206121, 0, '1,1'::text),
	substandard_created_time timestamp,
	substandard_updated_time timestamp,
	substandard_deleted_time timestamp,
	substandard_dw_created_time timestamp,
	substandard_dw_updated_time timestamp,
	substandard_status integer,
	substandard_id bigint,
	substandard_name varchar(500),
	substandard_description varchar(500),
	substandard_standard_id bigint
)
diststyle all
sortkey(substandard_dw_id);

create table alefdw.dim_sublevel
(
	sublevel_dw_id bigint default "identity"(206121, 0, '1,1'::text),
	sublevel_created_time timestamp,
	sublevel_updated_time timestamp,
	sublevel_deleted_time timestamp,
	sublevel_dw_created_time timestamp,
	sublevel_dw_updated_time timestamp,
	sublevel_status integer,
	sublevel_id bigint,
	sublevel_name varchar(500),
	sublevel_description varchar(500),
	sublevel_substandard_id bigint
)
diststyle all
sortkey(sublevel_dw_id);

-- DDL for assignment feature

create table if not exists alefdw.dim_assignment
(
	assignment_dw_id bigint default "identity"(135583, 0, '1,1'::text),
	assignment_created_time TIMESTAMP,
	assignment_updated_time TIMESTAMP,
	assignment_deleted_time TIMESTAMP,
	assignment_published_time TIMESTAMP,
	assignment_dw_created_time TIMESTAMP,
	assignment_dw_updated_time TIMESTAMP,
	assignment_id VARCHAR(36),
	assignment_title VARCHAR(100),
	assignment_description VARCHAR(250),
	assignment_max_score NUMERIC(10,4),
	assignment_attachment_fileName VARCHAR(100),
	assignment_attachment_path VARCHAR(200),
	assignment_graded BOOLEAN,
	assignment_allow_submission BOOLEAN,
	assignment_language VARCHAR(36),
	assignment_status INTEGER,
	assignment_created_by varchar(36),
    assignment_is_gradeable boolean,
    assignment_assignment_status VARCHAR(36),
    assignment_created_by VARCHAR(36),
    assignment_updated_by VARCHAR(36),
    assignment_published_on TIMESTAMP,
    assignment_attachment_required VARCHAR(36),
    assignment_comment_required VARCHAR(36),
    assignment_type VARCHAR(36),
    assignment_metadata_author VARCHAR(36),
    assignment_metadata_is_sa BOOLEAN,
    assignment_metadata_authored_date VARCHAR(36),
    assignment_metadata_language VARCHAR(36),
    assignment_metadata_format_type VARCHAR(36),
    assignment_metadata_lexile_level VARCHAR(36),
    assignment_metadata_difficulty_level VARCHAR(36),
    assignment_metadata_resource_type VARCHAR(36),
    assignment_metadata_knowledge_dimensions VARCHAR(36),
    assignment_is_ccl_assignment BOOLEAN,

    school_dw_id VARCHAR(36),
    tenant_dw_id VARCHAR(36)
)
distkey( school_dw_id )
sortkey(assignment_id);

create table if not exists alefdw_stage.rel_assignment_instance
(
	assignment_instance_staging_id bigint default "identity"(837165, 0, '1,1'::text),
	assignment_instance_created_time timestamp,
    assignment_instance_updated_time timestamp,
    assignment_instance_deleted_time timestamp,
	assignment_instance_dw_created_time timestamp,
    assignment_instance_dw_updated_time timestamp,
    assignment_instance_id varchar(36),
    assignment_instance_instructional_plan_id varchar(36),
    assignment_uuid varchar(36),
	assignment_instance_due_on timestamp,
	assignment_instance_allow_late_submission boolean,
    teacher_uuid varchar(36),
	assignment_instance_type varchar(10),
    grade_uuid varchar(36),
    subject_uuid varchar(36),
    class_uuid varchar(36),
    learning_path_uuid varchar(36),
    lo_uuid varchar(36),
    section_uuid varchar(36),
	assignment_instance_start_on timestamp,
    assignment_instance_status integer,
    student_uuid varchar(36),
    assignment_instance_student_id varchar(36),
	assignment_instance_student_status boolean,
    tenant_uuid varchar(36),
    assignment_instance_trimester_id varchar(36)
)
sortkey(grade_uuid, class_uuid, student_uuid);

create table if not exists alefdw.dim_assignment_instance
(
	assignment_instance_dw_id bigint default "identity"(837165, 0, '1,1'::text),
    assignment_instance_id varchar(36),
    assignment_instance_created_time timestamp,
    assignment_instance_updated_time timestamp,
    assignment_instance_deleted_time timestamp,
    assignment_instance_dw_created_time timestamp,
    assignment_instance_dw_updated_time timestamp,
    assignment_instance_instructional_plan_id varchar(36),
    assignment_instance_assignment_dw_id bigint,
    assignment_instance_due_on timestamp,
    assignment_instance_allow_late_submission boolean,
    assignment_instance_teacher_dw_id bigint,
    assignment_instance_type varchar(10),
    assignment_instance_grade_dw_id bigint,
    assignment_instance_subject_dw_id bigint,
    assignment_instance_class_dw_id bigint,
    assignment_instance_learning_path_dw_id bigint,
    assignment_instance_lo_dw_id bigint,
    assignment_instance_section_dw_id bigint,
    assignment_instance_start_on timestamp,
    assignment_instance_status integer,
    assignment_instance_student_id varchar(36),
    assignment_instance_student_dw_id bigint,
    assignment_instance_student_status boolean,
    assignment_instance_tenant_dw_id bigint,
    assignment_instance_trimester_id varchar(36)
)
sortkey( assignment_instance_grade_dw_id, assignment_instance_class_dw_id, assignment_instance_student_dw_id);

create table if not exists alefdw_stage.staging_assignment_submission
(
	assignment_submission_staging_id bigint default "identity"(837165, 0, '1,1'::text),
	assignment_submission_assignment_instance_uuid varchar(36),
	assignment_submission_date_dw_id integer,
	assignment_submission_dw_created_time timestamp,
	student_uuid varchar(36),
	assignment_submission_status integer,
	assignment_submission_id varchar(36),
	assignment_submission_student_attachment_fileName varchar(100),
	assignment_submission_student_attachment_path varchar(200),
	assignment_submission_student_comment varchar(256),
	assignment_submission_status_date timestamp,
	assignment_submission_teacherResponse_comment varchar(256),
	assignment_submission_teacherResponse_score integer,
	assignment_submission_teacherResponse_attachment_fileName varchar(100),
	assignment_submission_teacherResponse_attachment_path varchar(200),
	teacher_uuid varchar(36),
	tenant_uuid varchar(36)
)
sortkey(assignment_submission_date_dw_id, student_uuid,teacher_uuid);

create table if not exists alefdw.fact_assignment_submission
(
	assignment_submission_dw_id bigint default "identity"(837165, 0, '1,1'::text),
	assignment_submission_assignment_instance_dw_id bigint,
	assignment_submission_date_dw_id integer,
	assignment_submission_dw_created_time timestamp,
	assignment_submission_student_dw_id bigint,
	assignment_submission_status integer,
	assignment_submission_id varchar(36),
	assignment_submission_student_attachment_fileName varchar(100),
	assignment_submission_student_attachment_path varchar(200),
	assignment_submission_student_comment varchar(256),
	assignment_submission_status_date timestamp,
	assignment_submission_teacherResponse_comment varchar(256),
	assignment_submission_teacherResponse_score integer,
	assignment_submission_teacherResponse_attachment_fileName varchar(100),
	assignment_submission_teacherResponse_attachment_path varchar(200),
	assignment_submission_teacher_dw_id bigint,
	assignment_submission_tenant_dw_id bigint
)
sortkey(assignment_submission_date_dw_id, assignment_submission_student_dw_id,assignment_submission_teacher_dw_id);

-----------xxx-----------NoToBeApplied------CCL and Assignment Changes---------------------------xxx--------------------


---------------------------------Apply---------------17 December,2019 --------------------------------------------------
alter table alefdw.dim_school add column school_source_id VARCHAR(256);

create table alefdw_stage.staging_skill_content_unavailable (
  scu_staging_id BIGINT IDENTITY(1,1),
  scu_created_time TIMESTAMP,
  scu_dw_created_time TIMESTAMP,
  scu_date_dw_id BIGINT,
  tenant_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  skill_uuid VARCHAR(36)
);

create table alefdw.fact_skill_content_unavailable (
  scu_dw_id BIGINT IDENTITY(1,1),
  scu_created_time TIMESTAMP,
  scu_dw_created_time TIMESTAMP,
  scu_date_dw_id BIGINT,
  scu_tenant_dw_id BIGINT,
  scu_lo_dw_id BIGINT,
  scu_skill_dw_id BIGINT
);

create table alefdw_stage.staging_lesson_feedback (
  lesson_feedback_staging_id BIGINT IDENTITY(1,1),
  lesson_feedback_id VARCHAR(36),
  lesson_feedback_created_time TIMESTAMP,
  lesson_feedback_dw_created_time TIMESTAMP,
  lesson_feedback_date_dw_id BIGINT,
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  academic_year_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  curr_uuid VARCHAR(36),
  curr_grade_uuid VARCHAR(36),
  curr_subject_uuid VARCHAR(36),
  fle_ls_uuid VARCHAR(36),
  lesson_feedback_trimester_id VARCHAR(36),
  lesson_feedback_trimester_order Int,
  lesson_feedback_content_academic_year Int,
  lesson_feedback_rating Int,
  lesson_feedback_rating_text VARCHAR(36),
  lesson_feedback_has_comment boolean,
  lesson_feedback_is_cancelled boolean
);

create table alefdw.fact_lesson_feedback (
  lesson_feedback_staging_id BIGINT IDENTITY(1,1),
  lesson_feedback_id VARCHAR(36),
  lesson_feedback_created_time TIMESTAMP,
  lesson_feedback_dw_created_time TIMESTAMP,
  lesson_feedback_date_dw_id BIGINT,
  lesson_feedback_tenant_dw_id BIGINT,
  lesson_feedback_school_dw_id BIGINT,
  lesson_feedback_academic_year_dw_id BIGINT,
  lesson_feedback_grade_dw_id BIGINT,
  lesson_feedback_class_dw_id BIGINT,
  lesson_feedback_subject_dw_id BIGINT,
  lesson_feedback_student_dw_id BIGINT,
  lesson_feedback_lo_dw_id BIGINT,
  lesson_feedback_curr_dw_id BIGINT,
  lesson_feedback_curr_grade_dw_id BIGINT,
  lesson_feedback_curr_subject_dw_id BIGINT,
  lesson_feedback_fle_ls_dw_id BIGINT,
  lesson_feedback_trimester_id VARCHAR(36),
  lesson_feedback_trimester_order Int,
  lesson_feedback_content_academic_year Int,
  lesson_feedback_rating VARCHAR(50),
  lesson_feedback_rating_text VARCHAR(50),
  lesson_feedback_has_comment boolean,
  lesson_feedback_is_cancelled boolean
);

alter table alefdw.dim_subject add column grade_id varchar(36);
alter table alefdw_stage.rel_teacher add column teacher_source_identifier VARCHAR(256);
alter table alefdw.dim_teacher add column teacher_source_identifier VARCHAR(256);
alter table alefdw.dim_class add column class_source_id VARCHAR(256);
alter table alefdw.dim_learning_objective add column lo_curriculum_id varchar(36);
alter table alefdw.dim_learning_objective add column lo_curriculum_subject_id varchar(36);
alter table alefdw.dim_learning_objective add column lo_curriculum_grade_id varchar(36);
alter table alefdw.dim_learning_objective add column lo_content_academic_year varchar(50) ;
alter table alefdw.dim_learning_objective add column lo_order bigint;

----------------xxx--------------Apply---------------17 December,2019 ------------------xxx-----------------------------
alter table alefdw.dim_student add column student_special_needs varchar(2000);
alter table alefdw_stage.rel_student add column student_special_needs varchar(2000);


---------------------------------Apply------------------28 April 2020---------------------------------------------------

-- DDL for organisation
create table if not exists alefdw.dim_organisation
(
organisation_id varchar(36),
organisation_name varchar(20)
)
diststyle all
sortkey(organisation_id);
-- Insert statement for organisation
INSERT INTO alefdw.dim_organisation (organisation_id, organisation_name) VALUES ('25e9b735-6b6c-403b-a9f3-95e478e8f1ed', 'MOE');
INSERT INTO alefdw.dim_organisation (organisation_id, organisation_name) VALUES ('c35b7c6a-c3a3-47e5-a63c-cd25bdecc3ba', 'HCZ');
INSERT INTO alefdw.dim_organisation (organisation_id, organisation_name) VALUES ('b786009d-eb05-407e-a174-fc7e7e8ad515', 'KES');
INSERT INTO alefdw.dim_organisation (organisation_id, organisation_name) VALUES ('66ab3754-5103-485a-aacc-2c8d865e2497', 'Private Schools');
-- DDL for term
create table if not exists alefdw.dim_term
(
term_dw_id bigint IDENTITY(1,1),
term_created_time timestamp,
term_updated_time timestamp,
term_deleted_time timestamp,
term_dw_created_time timestamp,
term_dw_updated_time timestamp,
term_status int,
term_id varchar(36),
term_academic_period_order integer,
term_curriculum_id bigint,
term_content_academic_year_id bigint,
term_organisation_id varchar(36),
term_start_date date,
term_end_date date)
diststyle all
sortkey(term_dw_id);
-- DDL for week
create table if not exists alefdw.dim_week
(
	week_dw_id bigint IDENTITY(1,1),
	week_created_time timestamp,
	week_updated_time timestamp,
	week_deleted_time timestamp,
	week_dw_created_time timestamp,
	week_dw_updated_time timestamp,
	week_status int,
	week_id varchar(36),
	week_number integer,
	week_start_date date,
	week_end_date date,
	week_term_id varchar(36)
)
diststyle all
sortkey(week_dw_id);

-- DDL for Content academic year
create table if not exists alefdw.dim_content_academic_year
(
content_academic_year_id integer,
content_academic_year_name varchar(20)
)
diststyle all
sortkey(content_academic_year_id);

-- Insert statement for Content academic year
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (1, '2019');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (2, '2020');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (3, '2021');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (4, '2022');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (5, '2023');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (6, '2024');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (7, '2025');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (8, '2026');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (9, '2027');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (10, '2028');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (11, '2029');
INSERT INTO alefdw.dim_content_academic_year (content_academic_year_id, content_academic_year_name) VALUES (12, '2030');

---------------------------------Apply------------------28 April 2020---------------------------------------------------

------------------------------------21st may apply-----------------------------------------------
alter table alefdw.dim_class rename to dim_section;
alter table alefdw.dim_section rename column class_dw_id to section_dw_id;
alter table alefdw.dim_section rename column class_created_time to section_created_time;
alter table alefdw.dim_section rename column class_updated_time to section_updated_time;
alter table alefdw.dim_section rename column class_deleted_time to section_deleted_time;
alter table alefdw.dim_section rename column class_dw_created_time to section_dw_created_time;
alter table alefdw.dim_section rename column class_dw_updated_time to section_dw_updated_time;
alter table alefdw.dim_section rename column class_id to section_id;
alter table alefdw.dim_section rename column class_status to section_status;
alter table alefdw.dim_section rename column class_name to section_alias;
alter table alefdw.dim_section rename column class_section to section_name;
alter table alefdw.dim_section rename column class_enabled to section_enabled;
alter table alefdw.dim_section rename column class_source_id to section_source_id;

alter table alefdw.dim_class_schedule rename column rel_class_schedule_dw_id to rel_section_schedule_dw_id;
alter table alefdw.dim_class_schedule rename column class_id to section_id
alter table alefdw.dim_class_schedule rename column class_schedule_day_of_week to section_schedule_day_of_week;
alter table alefdw.dim_class_schedule rename column class_schedule_start_time to section_schedule_start_time;
alter table alefdw.dim_class_schedule rename column class_schedule_end_time to section_schedule_end_time;
alter table alefdw.dim_class_schedule rename column class_schedule_created_time to section_schedule_created_time;
alter table alefdw.dim_class_schedule rename column class_schedule_updated_time to section_schedule_updated_time;
alter table alefdw.dim_class_schedule rename column class_schedule_deleted_time to section_schedule_deleted_time;
alter table alefdw.dim_class_schedule rename column class_schedule_dw_created_time to section_schedule_dw_created_time;
alter table alefdw.dim_class_schedule rename column class_schedule_dw_updated_time to section_schedule_dw_updated_time;
alter table alefdw.dim_class_schedule rename column class_schedule_active_until to section_schedule_active_until;
alter table alefdw.dim_class_schedule rename column class_schedule_status to section_schedule_status;
alter table alefdw.dim_class_schedule rename column class_schedule_class_dw_id to section_schedule_section_dw_id;
alter table alefdw.dim_class_schedule rename column class_schedule_tenant_dw_id to section_schedule_tenant_dw_id;
alter table alefdw.dim_class_schedule rename column class_schedule_subject_dw_id to section_schedule_subject_dw_id;
alter table alefdw.dim_class_schedule rename column class_schedule_teacher_dw_id to section_schedule_teacher_dw_id;
alter table alefdw.dim_class_schedule rename to dim_section_schedule;

alter table alefdw_stage.rel_class_schedule rename column rel_class_schedule_id to rel_section_schedule_id;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_day_of_week to section_schedule_day_of_week;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_start_time to section_schedule_start_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_end_time to section_schedule_end_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_created_time to section_schedule_created_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_updated_time to section_schedule_updated_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_deleted_time to section_schedule_deleted_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_dw_created_time to section_schedule_dw_created_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_dw_updated_time to section_schedule_dw_updated_time;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_active_until to section_schedule_active_until;
alter table alefdw_stage.rel_class_schedule rename column class_schedule_status to section_schedule_status;
alter table alefdw_stage.rel_class_schedule rename column class_uuid to section_uuid;
alter table alefdw_stage.rel_class_schedule rename to rel_section_schedule;

alter table alefdw.dim_student rename column student_class_dw_id to student_section_dw_id;
alter table alefdw_stage.rel_student rename column class_uuid to section_uuid;
----------------xxxx----------------21st may apply-----------------------------------------------

------------------------------------8th June apply---------------------------------------------------
alter table alefdw_stage.staging_inc_game rename column class_uuid to section_uuid;
alter table alefdw.fact_inc_game rename column inc_game_class_dw_id to inc_game_section_dw_id;

alter table alefdw.fact_learning_experience rename column fle_class_dw_id to fle_section_dw_id;
alter table alefdw_stage.staging_learning_experience rename column class_uuid to section_uuid;

alter table alefdw.fact_practice rename column practice_class_dw_id to practice_section_dw_id;
alter table alefdw.fact_practice_session rename column practice_session_class_dw_id to practice_session_section_dw_id;
alter table alefdw_stage.staging_practice rename column class_uuid to section_uuid;
alter table alefdw_stage.staging_practice_session rename column class_uuid to section_uuid;

alter table alefdw_stage.staging_ktg rename column class_uuid to section_uuid;
alter table alefdw_stage.staging_ktg_session rename column class_uuid to section_uuid;
alter table alefdw_stage.staging_ktgskipped rename column class_uuid to section_uuid;
alter table alefdw.fact_ktg rename column ktg_class_dw_id to ktg_section_dw_id;
alter table alefdw.fact_ktg_session rename column ktg_session_class_dw_id to ktg_session_section_dw_id;
alter table alefdw.fact_ktgskipped rename column ktgskipped_class_dw_id to ktgskipped_section_dw_id;

alter table alefdw_stage.staging_lesson_feedback rename column class_uuid to section_uuid;
alter table alefdw.fact_lesson_feedback rename column lesson_feedback_class_dw_id to lesson_feedback_section_dw_id;
alter table alefdw_stage.staging_conversation_occurred rename column class_uuid to section_uuid;
alter table alefdw.fact_conversation_occurred rename column fco_class_dw_id to fco_section_dw_id;

alter table alefdw_stage.staging_student_activities rename column class_uuid to section_uuid;
alter table alefdw.fact_student_activities rename column fsta_class_dw_id to fsta_section_dw_id;
alter table alefdw_stage.staging_teacher_activities rename column class_uuid to section_uuid;
alter table alefdw.fact_teacher_activities rename column fta_class_dw_id to fta_section_dw_id;

create table if not exists alefdw_stage.rel_instructional_plan
(
rel_instructional_plan_id bigint IDENTITY(1,1),
instructional_plan_created_time timestamp,
instructional_plan_updated_time timestamp,
instructional_plan_deleted_time timestamp,
instructional_plan_dw_created_time timestamp,
instructional_plan_dw_updated_time timestamp,
instructional_plan_status int,
instructional_plan_id varchar(36),
instructional_plan_name varchar(100),
instructional_plan_curriculum_id bigint,
instructional_plan_curriculum_subject_id bigint,
instructional_plan_curriculum_grade_id bigint,
instructional_plan_content_academic_year_id int,
instructional_plan_organisation_id varchar(36),
instructional_plan_item_order integer,
week_uuid varchar(36),
lo_uuid varchar(36),
instructional_plan_item_ccl_lo_id bigint,
instructional_plan_item_optional boolean
)

create table if not exists alefdw.dim_instructional_plan
(
instructional_plan_dw_id bigint IDENTITY(1,1),
instructional_plan_created_time timestamp,
instructional_plan_updated_time timestamp,
instructional_plan_deleted_time timestamp,
instructional_plan_dw_created_time timestamp,
instructional_plan_dw_updated_time timestamp,
instructional_plan_status int,
instructional_plan_id varchar(36),
instructional_plan_name varchar(100),
instructional_plan_curriculum_id bigint,
instructional_plan_curriculum_subject_id bigint,
instructional_plan_curriculum_grade_id bigint,
instructional_plan_content_academic_year_id int,
instructional_plan_organisation_id varchar(36),
instructional_plan_item_order integer,
instructional_plan_item_week_dw_id BIGINT,
instructional_plan_item_lo_dw_id BIGINT,
instructional_plan_item_ccl_lo_id bigint,
instructional_plan_item_optional boolean
)
diststyle all
sortkey(instructional_plan_dw_id);

----------------xxxx----------------8th June apply--------------------xxxx---------------------------


-------------------- Not to be applied till BI adapt to new Term and curriculum changes-------------------------------------
create table if not exists alefdw.dim_curr(
   curr_id BIGINT NOT NULL,
   curr_name VARCHAR(255)
)
 diststyle all
 compound sortkey(curr_id);

insert into alefdw.dim_curr (curr_id, curr_name) VALUES (392027, 'UAE MOE'), (563622, 'NYDOE'), (785844, 'NS');
--------xxx------------ Not to be applied till BI adapt to new Term and curriculum changes---------------xxx----------------------

------------------------------------12th July apply---------------------------------------------------

-- DDL changes for Lesson/MLO:
alter table alefdw.dim_learning_objective add column lo_ccl_id bigint;
alter table alefdw.dim_learning_objective add column lo_action_status integer;
alter table alefdw.dim_learning_objective add column lo_description varchar(1500);
alter table alefdw.dim_learning_objective add column lo_skillable boolean;
alter table alefdw.dim_learning_objective add column lo_framework_id integer;
alter table alefdw.dim_learning_objective add column lo_user_id integer;
alter table alefdw.dim_learning_objective add column lo_template_id integer;
alter table alefdw.dim_learning_objective add column lo_content_academic_year_id integer;
alter table alefdw.dim_learning_objective add column lo_assessment_tool varchar(50);
alter table alefdw.dim_learning_objective rename column lo_type to lo_framework_code;
alter table alefdw.dim_learning_objective add column lo_type varchar(100);
alter table alefdw.dim_learning_objective drop column lo_order;
alter table alefdw.dim_learning_objective add column lo_publisher_id integer;
alter table alefdw.dim_learning_objective add column lo_published_date date;
alter table alefdw.dim_learning_objective add column lo_max_stars integer;
alter table alefdw.dim_learning_objective add column lo_theme_id bigint;
alter table alefdw.dim_learning_objective add column lo_duration integer;
alter table alefdw.dim_learning_objective alter column lo_code type varchar(750);
alter table alefdw.dim_learning_objective alter column lo_title type varchar(750);

-- DDL for LO association
CREATE TABLE alefdw.dim_learning_objective_association
(
  lo_association_dw_id BIGINT IDENTITY(1,1),
  lo_association_lo_id VARCHAR(36),
  lo_association_created_time TIMESTAMP,
  lo_association_updated_time TIMESTAMP,
  lo_association_dw_created_time TIMESTAMP,
  lo_association_dw_updated_time TIMESTAMP,
  lo_association_status INT,
  lo_association_step_id INT,
  lo_association_lo_ccl_id BIGINT,
  lo_association_id VARCHAR(36),
  lo_association_attach_status INT,
  lo_association_type INT
)
  diststyle all
  sortkey(lo_association_dw_id);


-- DDL for Framework
create table if not exists alefdw.dim_framework
(
  framework_id integer,
  framework_title varchar(50),
  framework_description varchar(200),
  framework_name varchar(50),
  framework_code varchar(36),
  framework_step_id integer
)
  diststyle all
  sortkey(framework_id);

-- Insert for Framework
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',1);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',2);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',3);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',4);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',5);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',6);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',7);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',8);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',9);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',10);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',11);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',12);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',13);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',14);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',15);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',16);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',17);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',18);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',19);
INSERT INTO alefdw.dim_framework (framework_id, framework_title, framework_description, framework_name, framework_code,framework_step_id) VALUES (1, 'Generic Framework', 'FULL FRAMEWORK WITH 3 DOKs','ADEC','FF4',20);

-- DDL for Template
create table if not exists alefdw.dim_template
(
  template_id integer,
  template_framework_id integer,
  template_title varchar(50),
  template_description varchar(200),
  template_step_id integer,
  template_step_display_name varchar(200)
)
  diststyle all
  sortkey(template_id);

-- Insert for Template
-- template will be inserted by https://dbc-eecd55fa-0223.cloud.databricks.com/#notebook/554447/command/554448 from csv.

-- DDL for Step
create table if not exists alefdw.dim_step
(
  step_id integer,
  step_code varchar(100),
  step_content_type varchar(50),
  step_framework_display_name varchar(50),
  step_short_code varchar(36),
  step_icon_term varchar(36),
  step_required boolean,
  step_flag varchar(50),
  step_retry boolean
)
  diststyle all
  sortkey(step_id);

-- Insert for Step
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (1,'ANTICIPATORY_CONTENT', 'LEARNING', 'The Big Idea', 'AC', 'icon-the-big-idea',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (2,'METACOGNITIVE_PROMPT', 'LEARNING', 'My Thinking Plan', 'MP', 'icon-my-thinking-plan',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (3,'PREREQUISITE', 'LEARNING', 'My Background Knowledge', 'PRE', 'icon-my-background-knowledge',false,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (4,'KEY_TERMS', 'LEARNING', 'Word Bank', 'KT', 'icon-word-bank',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (5,'LESSON_1', 'LEARNING', 'Depth of Knowledge 1', 'L1', 'icon-depth-of-knowledge-2',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (6,'TEQ_1', 'ASSESSMENT', 'Check My Understanding', 'TEQ1', 'icon-what-i-know',true,'none',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (7,'REMEDIATION_1', 'LEARNING', 'Second Look', 'REM1', 'icon-second-look',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (8,'TEQ_1_1', 'ASSESSMENT', 'Check My Understanding', 'TEQ_1_1', 'icon-what-i-know',true,'YELLOW',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (9,'LESSON_2', 'LEARNING', 'Depth of Knowledge 2', 'L2', 'icon-depth-of-knowledge-2',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (10,'TEQ_2', 'ASSESSMENT', 'Check My Understanding', 'TEQ_2', 'icon-what-i-know',true,'none',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (11,'REMEDIATION_2', 'LEARNING', 'Second Look', 'REM2', 'icon-second-look',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (12,'TEQ_2_1', 'ASSESSMENT', 'Check My Understanding', 'TEQ_2_1', 'icon-what-i-know',true,'ORANGE',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (13,'LESSON_3', 'LEARNING', 'Depth of Knowledge 3', 'L3', 'icon-depth-of-knowledge-2',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (14,'TEQ_3', 'ASSESSMENT', 'Check My Understanding', 'TEQ_3', 'icon-what-i-know',true,'none',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (15,'REMEDIATION_3', 'LEARNING', 'Second Look', 'REM3', 'icon-second-look',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (16,'TEQ_3_1', 'ASSESSMENT', 'Check My Understanding', 'TEQ_3_1', 'icon-what-i-know',true,'PURPLE',true);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (17,'SUMMARY', 'LEARNING', 'Last Look', 'SUM', 'icon-last-look',true,'none',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (18,'SUMMATIVE_ASSESSMENT', 'ASSESSMENT', 'My Exit Ticket', 'SA', 'icon-my-exit-ticket',true,'TURQUOISE',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (19,'ENHANCEMENT_ACTIVITY', 'LEARNING', 'Enhancement Activity', 'ENH_A', 'icon-enhancement-activity',false,'TURQUOISE',false);
INSERT INTO alefdw.dim_step (step_id, step_code, step_content_type, step_framework_display_name, step_short_code, step_icon_term, step_required, step_flag, step_retry) VALUES (20,'ENRICHMENT_ACTIVITY', 'LEARNING', 'Enrichment Activity', 'ENR_A', 'icon-enrichment-activity',true,'TURQUOISE',false);


--CCL CHANGES To be applied independently-----------------------
-- Alter for dim_curriculum
alter table alefdw.dim_curriculum add column curr_created_time timestamp;
alter table alefdw.dim_curriculum add column curr_updated_time timestamp;
alter table alefdw.dim_curriculum add column curr_deleted_time timestamp;
alter table alefdw.dim_curriculum add column curr_dw_created_time timestamp;
alter table alefdw.dim_curriculum add column curr_dw_updated_time timestamp;
alter table alefdw.dim_curriculum add column curr_status integer;
-- Alter for dim_curriculum_grade
alter table alefdw.dim_curriculum_grade add column curr_grade_created_time timestamp;
alter table alefdw.dim_curriculum_grade add column curr_grade_updated_time timestamp;
alter table alefdw.dim_curriculum_grade add column curr_grade_deleted_time timestamp;
alter table alefdw.dim_curriculum_grade add column curr_grade_dw_created_time timestamp;
alter table alefdw.dim_curriculum_grade add column curr_grade_dw_updated_time timestamp;
alter table alefdw.dim_curriculum_grade add column curr_grade_status integer;
alter table alefdw.dim_curriculum_grade alter column curr_grade_name type varchar(255);
-- Alter for dim_curriculum_subject
alter table alefdw.dim_curriculum_subject add column curr_subject_created_time timestamp;
alter table alefdw.dim_curriculum_subject add column curr_subject_updated_time timestamp;
alter table alefdw.dim_curriculum_subject add column curr_subject_deleted_time timestamp;
alter table alefdw.dim_curriculum_subject add column curr_subject_dw_created_time timestamp;
alter table alefdw.dim_curriculum_subject add column curr_subject_dw_updated_time timestamp;
alter table alefdw.dim_curriculum_subject add column curr_subject_status integer;
alter table alefdw.dim_curriculum_subject add column curr_subject_skillable boolean;


-- DDL for Content table
create table if not exists alefdw.dim_content
(
  content_dw_id bigint IDENTITY(1,1),
  content_created_time timestamp,
  content_updated_time timestamp,
  content_deleted_time timestamp,
  content_dw_created_time timestamp,
  content_dw_updated_time timestamp,
  content_status integer,
  content_id varchar(36),
  content_title varchar(750),
  content_tags varchar(255),
  content_file_name varchar(750),
  content_file_content_type varchar(50),
  content_file_size bigint,
  content_file_updated_at timestamp,
  content_condition_of_use varchar(750),
  content_knowledge_dimension varchar(50),
  content_difficulty_level varchar(50),
  content_language varchar(50),
  content_lexical_level varchar(36),
  content_media_type varchar(50),
  content_action_status integer,
  content_location varchar(100),
  content_authored_date date,
  content_created_at timestamp,
  content_created_by bigint,
  content_published_date date,
  content_publisher_id bigint
)
  diststyle all
  sortkey(content_dw_id);

  ------------  Adding IP columns facts -----------------

alter table alefdw.fact_learning_experience add column fle_instructional_plan_id VARCHAR(36);
alter table alefdw_stage.staging_learning_experience add column fle_instructional_plan_id VARCHAR(36);

alter table alefdw.fact_practice add column practice_instructional_plan_id VARCHAR(36) ;
alter table alefdw.fact_practice add column practice_learning_path_id VARCHAR(36) ;

alter table alefdw.fact_practice_session add column practice_session_instructional_plan_id VARCHAR(36) ;
alter table alefdw.fact_practice_session add column practice_session_learning_path_id VARCHAR(36) ;


alter table alefdw_stage.staging_practice add column practice_instructional_plan_id VARCHAR(36) ;
alter table alefdw_stage.staging_practice add column practice_learning_path_id VARCHAR(36) ;

alter table alefdw_stage.staging_practice_session add column practice_session_instructional_plan_id VARCHAR(36) ;
alter table alefdw_stage.staging_practice_session add column practice_seesion_learning_path_id VARCHAR(36) ;


------------  Adding IP columns to inc-game event -----------------
alter table alefdw.fact_inc_game add column inc_game_instructional_plan_id varchar(36);
alter table alefdw_stage.staging_inc_game add column inc_game_instructional_plan_id varchar(36);

------------  Adding IP columns to Lesson feedback -----------------
alter table alefdw.fact_lesson_feedback add column lesson_feedback_instructional_plan_id varchar(36);
alter table alefdw_stage.staging_lesson_feedback add column lesson_feedback_instructional_plan_id varchar(36);
alter table alefdw.fact_lesson_feedback add column lesson_feedback_learning_path_id varchar(36);
alter table alefdw_stage.staging_lesson_feedback add column lesson_feedback_learning_path_id varchar(36);

------------  Adding IP columns to KT games  -----------------
alter table alefdw.fact_ktg add column ktg_instructional_plan_id varchar(36);
alter table alefdw.fact_ktg add column ktg_learning_path_id varchar(36);
alter table alefdw.fact_ktg_session add column ktg_session_instructional_plan_id varchar(36);
alter table alefdw.fact_ktg_session add column ktg_session_learning_path_id varchar(36);
alter table alefdw.fact_ktgskipped add column ktgskipped_instructional_plan_id varchar(36);
alter table alefdw.fact_ktgskipped add column ktgskipped_learning_path_id varchar(36);

alter table alefdw_stage.staging_ktg add column ktg_instructional_plan_id varchar(36);
alter table alefdw_stage.staging_ktg add column ktg_learning_path_id varchar(36);
alter table alefdw_stage.staging_ktg_session add column ktg_session_instructional_plan_id varchar(36);
alter table alefdw_stage.staging_ktg_session add column ktg_session_learning_path_id varchar(36);
alter table alefdw_stage.staging_ktgskipped add column ktgskipped_instructional_plan_id varchar(36);
alter table alefdw_stage.staging_ktgskipped add column ktgskipped_learning_path_id varchar(36);

alter table   alefdw.dim_curriculum add column curr_id_new bigint;
update        alefdw.dim_curriculum set curr_id_new=cast(curr_id as bigint);
alter table   alefdw.dim_curriculum drop column curr_id;
alter table   alefdw.dim_curriculum rename column curr_id_new to  curr_id;
alter table   alefdw.dim_curriculum_grade add column curr_grade_id_new bigint;
update        alefdw.dim_curriculum_grade set curr_grade_id_new=cast(curr_grade_id as bigint);
alter table   alefdw.dim_curriculum_grade drop column curr_grade_id;
alter table   alefdw.dim_curriculum_grade rename column curr_grade_id_new to  curr_grade_id;
alter table   alefdw.dim_curriculum_subject add column curr_subject_id_new bigint;
update        alefdw.dim_curriculum_subject set curr_subject_id_new=cast(curr_subject_id as bigint);
alter table   alefdw.dim_curriculum_subject drop column curr_subject_id;
alter table   alefdw.dim_curriculum_subject rename column curr_subject_id_new to  curr_subject_id;

alter table   alefdw.dim_learning_objective add column curr_id_new bigint;
update        alefdw.dim_learning_objective set curr_id_new=cast(lo_curriculum_id as bigint);
alter table   alefdw.dim_learning_objective drop column lo_curriculum_id;
alter table   alefdw.dim_learning_objective rename column curr_id_new to  lo_curriculum_id;
alter table   alefdw.dim_learning_objective add column curr_grade_id_new bigint;
update        alefdw.dim_learning_objective set curr_grade_id_new=cast(lo_curriculum_grade_id as bigint);
alter table   alefdw.dim_learning_objective drop column lo_curriculum_grade_id;
alter table   alefdw.dim_learning_objective rename column curr_grade_id_new to  lo_curriculum_grade_id;
alter table   alefdw.dim_learning_objective add column curr_subject_id_new bigint;
update        alefdw.dim_learning_objective set curr_subject_id_new=cast(lo_curriculum_subject_id as bigint);
alter table   alefdw.dim_learning_objective drop column lo_curriculum_subject_id;
alter table   alefdw.dim_learning_objective rename column curr_subject_id_new to  lo_curriculum_subject_id;
alter table   alefdw.dim_content add column content_id_new bigint;
update        alefdw.dim_content set content_id_new=cast(content_id as bigint);
alter table   alefdw.dim_content drop column content_id;
alter table   alefdw.dim_content rename column content_id_new to  content_id;

-- dim_lesson_template
--
-- do initial load first
-- drop static dim_template after initial load and rename dim_lesson_template to dim_template
--

CREATE TABLE IF NOT EXISTS alefdw.dim_template
(
    template_dw_id BIGINT IDENTITY(1, 1),
    template_status SMALLINT,
    template_created_time TIMESTAMP,
    template_updated_time TIMESTAMP,
    template_deleted_time TIMESTAMP,
    template_dw_created_time TIMESTAMP,
    template_dw_updated_time TIMESTAMP,

    template_id BIGINT,
    template_framework_id BIGINT,
    template_title VARCHAR(750), -- limited to 250 characters * 3 bytes to account for arabic characters
    template_step_id BIGINT,
    template_step_display_name VARCHAR(90) -- limited to 30 characters * 3 bytes to account for arabic characters
)
DISTSTYLE ALL
SORTKEY(template_dw_id);
----------------xxxx----------------12th July apply--------------------xxxx---------------------------

----------------------------------CBM changes---------------------------------------------------------
alter table alefdw.fact_learning_experience add column fle_class_dw_id bigint;
alter table alefdw_stage.staging_learning_experience add column class_uuid varchar(36);

alter table alefdw_stage.staging_practice add column class_uuid varchar(36);
alter table alefdw_stage.staging_practice_session add column class_uuid varchar(36);
alter table alefdw.fact_practice add column practice_class_dw_id bigint;
alter table alefdw.fact_practice_session add column practice_session_class_dw_id bigint;

alter table alefdw.fact_lesson_feedback add column lesson_feedback_class_dw_id bigint;
alter table alefdw_stage.staging_lesson_feedback add column class_uuid varchar(36);

alter table alefdw_stage.staging_ktg add column class_uuid varchar(36);
alter table alefdw_stage.staging_ktg_session add column class_uuid varchar(36);
alter table alefdw_stage.staging_ktgskipped add column class_uuid varchar(36);
alter table alefdw.fact_ktg add column ktg_class_dw_id bigint;
alter table alefdw.fact_ktg_session add column ktg_session_class_dw_id bigint;
alter table alefdw.fact_ktgskipped add column ktgskipped_class_dw_id bigint;

ALTER TABLE alefdw.fact_inc_game ADD COLUMN inc_game_class_dw_id BIGINT;
ALTER TABLE alefdw_stage.staging_inc_game ADD COLUMN class_uuid VARCHAR(36);

alter table alefdw_stage.staging_star_awarded add column class_uuid varchar(36);
alter table alefdw.fact_star_awarded add column fsa_class_dw_id BIGINT;

alter table alefdw.dim_learning_path add column learning_path_class_id VARCHAR(36);

alter table alefdw.dim_learning_objective drop column lo_description;

create table if not exists alefdw.dim_class
(
class_dw_id bigint IDENTITY(1,1),
class_created_time timestamp,
class_updated_time timestamp,
class_deleted_time timestamp,
class_dw_created_time timestamp,
class_dw_updated_time timestamp,
class_status int,
class_id varchar(36),
class_title varchar(255),
class_school_id varchar(36),
class_grade_id varchar(36),
class_section_id varchar(36),
class_academic_year_id varchar(36),
class_gen_subject varchar(255) ,
class_curriculum_id bigint,
class_curriculum_grade_id bigint,
class_curriculum_subject_id bigint,
class_content_academic_year int,
class_tutor_dhabi_enabled boolean,
class_language_direction varchar(25),
class_online boolean,
class_practice boolean,
class_course_status varchar(50),
class_source_id varchar(255)
)
diststyle all
sortkey(class_dw_id);

create table if not exists alefdw.dim_class_user(
rel_class_user_dw_id bigint IDENTITY(1,1),
class_user_created_time TIMESTAMP,
class_user_updated_time TIMESTAMP,
class_user_deleted_time TIMESTAMP,
class_user_dw_created_time TIMESTAMP,
class_user_dw_updated_time TIMESTAMP,
class_user_active_until TIMESTAMP,
class_user_status INT,
class_user_class_dw_id bigint,
class_user_user_dw_id bigint,
class_user_role_dw_id bigint
)
diststyle all
sortkey(class_user_class_dw_id,class_user_user_dw_id);

create table if not exists alefdw_stage.rel_class_user(
rel_class_user_id bigint IDENTITY(1,1),
class_user_created_time TIMESTAMP,
class_user_updated_time TIMESTAMP,
class_user_deleted_time TIMESTAMP,
class_user_dw_created_time TIMESTAMP,
class_user_dw_updated_time TIMESTAMP,
class_user_active_until TIMESTAMP,
class_user_status INT,
class_uuid varchar(36),
user_uuid varchar(36),
role_uuid varchar(20)
)

CREATE TABLE alefdw.dim_content_association
(
  content_association_dw_id BIGINT IDENTITY(1,1),
  content_association_content_id BIGINT,
  content_association_created_time TIMESTAMP,
  content_association_updated_time TIMESTAMP,
  content_association_dw_created_time TIMESTAMP,
  content_association_dw_updated_time TIMESTAMP,
  content_association_status INT,
  content_association_id VARCHAR(36),
  content_association_type INT,
  content_association_attach_status INT
)
diststyle all
sortkey(content_association_dw_id);

-- Data model for outcome

-- DDL for outcome table
create table alefdw.dim_outcome
(
    outcome_dw_id BIGINT IDENTITY(1,1),
    outcome_created_time timestamp,
    outcome_updated_time timestamp,
    outcome_deleted_time timestamp,
    outcome_dw_created_time timestamp,
    outcome_dw_updated_time timestamp,
    outcome_status integer,
    outcome_id varchar(36),
    outcome_parent_id varchar(36),
    outcome_type integer,
    outcome_name varchar(1500),
    outcome_description varchar(1500),
    outcome_curriculum_id bigint,
    outcome_curriculum_grade_id bigint,
    outcome_curriculum_subject_id bigint
)
diststyle all
sortkey(outcome_dw_id);

alter table alefdw.dim_organisation alter column organisation_name type varchar(250);
insert into alefdw.dim_organisation (organisation_id, organisation_name) values ('53055d6d-ecf6-4596-88db-0c50cac72cd0','MoE-North (Public)');
UPDATE alefdw.dim_organisation SET organisation_name = 'MoE-Abu Dhabi (Public)' WHERE organisation_id = '25e9b735-6b6c-403b-a9f3-95e478e8f1ed';
UPDATE alefdw.dim_organisation SET organisation_name = 'MoE-North (Private)' WHERE organisation_id = '66ab3754-5103-485a-aacc-2c8d865e2497';
insert into alefdw.dim_organisation (organisation_id, organisation_name) values ('50445c7f-0f0d-45d4-aee1-26919a9b50f5','MSCL')

------------------------xxxx--------------- CBM changes --------------------------------xxxx----------------------------
--------------------------------------- FIX TERM QUERY APPLIED 21 August------------------------------------------------
alter table alefdw.fact_learning_experience rename fle_curr_dw_id to fle_term_dw_id;
alter table alefdw.fact_lesson_feedback rename lesson_feedback_curr_dw_id to lesson_feedback_term_dw_id;
alter table alefdw.dim_curriculum drop column curr_academic_period_order;
alter table alefdw.dim_curriculum drop column curr_content_academic_year;
alter table alefdw.dim_curriculum drop column curr_academic_period_start_date;
alter table alefdw.dim_curriculum drop column curr_academic_period_end_date;
------------------------xxxx----------- FIX TERM QUERY APPLIED 21 August ---------------xxxx----------------------------

---------------------------xxxx Sept End/Oct 1st apply------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alefdw.dim_theme
(
    theme_dw_id BIGINT IDENTITY(1, 1),
    theme_status SMALLINT,
    theme_created_time TIMESTAMP,
    theme_updated_time TIMESTAMP,
    theme_deleted_time TIMESTAMP,
    theme_dw_created_time TIMESTAMP,
    theme_dw_updated_time TIMESTAMP,

    theme_id BIGINT,
    theme_curriculum_id BIGINT,
    theme_curriculum_grade_id BIGINT,
    theme_curriculum_subject_id BIGINT,
    theme_name VARCHAR(750), -- limited to 250 characters * 3 bytes to account for arabic characters
    theme_created_at TIMESTAMP, -- upstream db create time
    theme_updated_at TIMESTAMP -- upstream db update time
)
DISTSTYLE ALL
SORTKEY(theme_dw_id);

CREATE TABLE alefdw.dim_team
(
  team_dw_id BIGINT IDENTITY(1,1),
  team_created_time TIMESTAMP,
  team_updated_time TIMESTAMP,
  team_deleted_time TIMESTAMP,
  team_dw_created_time TIMESTAMP,
  team_dw_updated_time TIMESTAMP,
  team_status INT,
  team_id VARCHAR(36),
  team_name VARCHAR(36),
  team_class_id VARCHAR(36),
  team_teacher_id VARCHAR(36)
)
diststyle all
sortkey(team_dw_id);

alter table alefdw.dim_team alter column team_name type varchar(255);


CREATE TABLE alefdw_stage.rel_team_student_association
(
  rel_team_student_association_id BIGINT IDENTITY(1,1),
  team_student_association_created_time TIMESTAMP,
  team_student_association_updated_time TIMESTAMP,
  team_student_association_dw_created_time TIMESTAMP,
  team_student_association_dw_updated_time TIMESTAMP,
  team_student_association_status SMALLINT,
  team_student_association_active_until TIMESTAMP,
  team_uuid VARCHAR(36),
  student_uuid VARCHAR(36)
);

CREATE TABLE alefdw.dim_team_student_association
(
  rel_team_student_association_dw_id BIGINT IDENTITY(1,1),
  team_student_association_created_time TIMESTAMP,
  team_student_association_updated_time TIMESTAMP,
  team_student_association_dw_created_time TIMESTAMP,
  team_student_association_dw_updated_time TIMESTAMP,
  team_student_association_status SMALLINT,
  team_student_association_active_until TIMESTAMP,
  team_student_association_team_dw_id BIGINT,
  team_student_association_student_dw_id BIGINT
)
DISTSTYLE ALL
SORTKEY(rel_team_student_association_dw_id);

CREATE TABLE alefdw.dim_team_association
(
  team_association_dw_id BIGINT IDENTITY(1,1),
  team_association_created_time TIMESTAMP,
  team_association_updated_time TIMESTAMP,
  team_association_dw_created_time TIMESTAMP,
  team_association_dw_updated_time TIMESTAMP,
  team_association_status INT,
  team_association_active_until TIMESTAMP,
  team_association_team_id VARCHAR(36),
  team_association_student_id VARCHAR(36)
)
diststyle all
sortkey(team_association_dw_id);

CREATE TABLE alefdw.dim_content_student_association
(
  content_student_association_dw_id BIGINT IDENTITY(1,1),
  content_student_association_created_time TIMESTAMP,
  content_student_association_updated_time TIMESTAMP,
  content_student_association_dw_created_time TIMESTAMP,
  content_student_association_dw_updated_time TIMESTAMP,
  content_student_association_status INT,
  content_student_association_content_id VARCHAR(36),
  content_student_association_student_id VARCHAR(36),
  content_student_association_assign_status INT
)
diststyle all
sortkey(content_student_association_dw_id);

CREATE TABLE alefdw.dim_question_pool
(
  question_pool_dw_id BIGINT IDENTITY(1,1),
  question_pool_created_time TIMESTAMP,
  question_pool_updated_time TIMESTAMP,
  question_pool_deleted_time TIMESTAMP,
  question_pool_dw_created_time TIMESTAMP,
  question_pool_dw_updated_time TIMESTAMP,
  question_pool_status SMALLINT,
  question_pool_id VARCHAR(36),
  question_pool_name VARCHAR(120),
  question_pool_action_status SMALLINT,
  question_pool_triggered_by BIGINT,
  question_pool_question_code_prefix VARCHAR(120)
)
diststyle all
sortkey(question_pool_dw_id);

CREATE TABLE alefdw_stage.rel_question_pool_association
(
    rel_question_pool_association_id BIGINT IDENTITY(1,1),
    question_pool_association_created_time TIMESTAMP,
    question_pool_association_updated_time TIMESTAMP,
    question_pool_association_dw_created_time TIMESTAMP,
    question_pool_association_dw_updated_time TIMESTAMP,
    question_pool_association_status INT,
    question_pool_association_assign_status INT,
    question_pool_association_question_code VARCHAR(120),
    question_pool_uuid VARCHAR(36),
    question_pool_association_triggered_by VARCHAR(100)
)
    diststyle all
sortkey(rel_question_pool_association_id);

CREATE TABLE alefdw.dim_question_pool_association
(
    question_pool_association_dw_id BIGINT IDENTITY(1,1),
    question_pool_association_created_time TIMESTAMP,
    question_pool_association_updated_time TIMESTAMP,
    question_pool_association_dw_created_time TIMESTAMP,
    question_pool_association_dw_updated_time TIMESTAMP,
    question_pool_association_status INT,
    question_pool_association_assign_status INT,
    question_pool_association_question_code VARCHAR(120),
    question_pool_association_question_pool_dw_id BIGINT,
    question_pool_association_triggered_by VARCHAR(100)
)
    diststyle all
sortkey(question_pool_association_dw_id);

alter table alefdw.dim_outcome drop column outcome_description;

CREATE TABLE alefdw.dim_curriculum_version
(
  curriculum_version_dw_id BIGINT IDENTITY(1,1),
  curriculum_version_created_time TIMESTAMP,
  curriculum_version_updated_time TIMESTAMP,
  curriculum_version_deleted_time TIMESTAMP,
  curriculum_version_dw_created_time TIMESTAMP,
  curriculum_version_dw_updated_time TIMESTAMP,
  curriculum_version_status SMALLINT,

  curriculum_version_id BIGINT,
  curriculum_version_curriculum_id BIGINT,
  curriculum_version_curriculum_grade_id BIGINT,
  curriculum_version_curriculum_subject_id BIGINT,
  curriculum_version_name VARCHAR(90), -- limited to 30 characters * 3 bytes to account for arabic characters
  curriculum_version_active_from DATE, -- upstream type is of https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
  curriculum_version_active_to DATE, -- upstream type is of https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
  curriculum_version_active BOOLEAN
)
DISTSTYLE ALL
SORTKEY(
    curriculum_version_dw_id,
    curriculum_version_id,
    curriculum_version_curriculum_id,
    curriculum_version_curriculum_grade_id,
    curriculum_version_curriculum_subject_id
);

CREATE TABLE alefdw.dim_outcome_association
(
outcome_association_dw_id BIGINT IDENTITY(1,1),
outcome_association_created_time TIMESTAMP,
outcome_association_updated_time TIMESTAMP,
outcome_association_dw_created_time TIMESTAMP,
outcome_association_dw_updated_time TIMESTAMP,
outcome_association_status INT,
outcome_association_outcome_id VARCHAR(36),
outcome_association_id VARCHAR(36),
outcome_association_type INT,
outcome_association_attach_status INT
)
diststyle all
sortkey(outcome_association_dw_id);

CREATE TABLE alefdw.dim_category
(
category_dw_id BIGINT IDENTITY(1,1),
category_created_time TIMESTAMP,
category_updated_time TIMESTAMP,
category_deleted_time TIMESTAMP,
category_dw_created_time TIMESTAMP,
category_dw_updated_time TIMESTAMP,
category_status INT,
category_id VARCHAR(36),
category_name VARCHAR(500),
category_code VARCHAR(150),
category_description VARCHAR(500)
)
diststyle all
sortkey(category_dw_id);

CREATE TABLE alefdw.dim_ccl_skill
(
ccl_skill_dw_id BIGINT IDENTITY(1,1),
ccl_skill_created_time TIMESTAMP,
ccl_skill_updated_time TIMESTAMP,
ccl_skill_deleted_time TIMESTAMP,
ccl_skill_dw_created_time TIMESTAMP,
ccl_skill_dw_updated_time TIMESTAMP,
ccl_skill_status INT,
ccl_skill_id VARCHAR(36),
ccl_skill_name VARCHAR(750),
ccl_skill_code VARCHAR(150),
ccl_skill_description VARCHAR(750),
ccl_skill_subject_id BIGINT
)
diststyle all
sortkey(ccl_skill_dw_id);

CREATE TABLE alefdw.dim_skill_association
(
skill_association_dw_id BIGINT IDENTITY(1,1),
skill_association_created_time TIMESTAMP,
skill_association_updated_time TIMESTAMP,
skill_association_dw_created_time TIMESTAMP,
skill_association_dw_updated_time TIMESTAMP,
skill_association_status INT,
skill_association_skill_id VARCHAR(36),
skill_association_skill_code VARCHAR(50),
skill_association_id VARCHAR(36),
skill_association_code VARCHAR(50),
skill_association_type INT,
skill_association_attach_status INT
)
diststyle all
sortkey(skill_association_dw_id);

alter table alefdw_stage.staging_conversation_occurred add column fco_subject_category varchar(256);
alter table alefdw.fact_conversation_occurred add column fco_subject_category varchar(256);
alter table alefdw_stage.staging_learning_experience add column fle_lesson_category varchar(40);
alter table alefdw.fact_learning_experience add column fle_lesson_category varchar(40);
---------------------------xxxx Sept End/Oct 1st apply------------------------------------------------------------------

CREATE TABLE alefdw.dim_tag
(
  tag_dw_id BIGINT IDENTITY(1,1),
  tag_created_time TIMESTAMP,
  tag_updated_time TIMESTAMP,
  tag_dw_created_time TIMESTAMP,
  tag_dw_updated_time TIMESTAMP,
  tag_id VARCHAR(36),
  tag_name VARCHAR(1024),
  tag_status INT,
  tag_type VARCHAR(36),
  tag_association_id VARCHAR(36),
  tag_association_dw_id BIGINT,
  tag_association_type INT,
  tag_association_attach_status INT
)
  diststyle all
  sortkey(tag_dw_id);

CREATE TABLE alefdw.dim_class_schedule
(
    class_schedule_dw_id BIGINT IDENTITY(1,1),
    class_schedule_created_time TIMESTAMP,
    class_schedule_updated_time TIMESTAMP,
    class_schedule_dw_created_time TIMESTAMP,
    class_schedule_dw_updated_time TIMESTAMP,
    class_schedule_status INT,
    class_schedule_class_id VARCHAR(36),
    class_schedule_day VARCHAR(10),
    class_schedule_start_time VARCHAR(10),
    class_schedule_end_time VARCHAR(10),
    class_schedule_attach_status INT
)
    diststyle all
    sortkey(class_schedule_dw_id);
---------------------------xxxx Oct End/Nov ~1st apply------------------------------------------------------------------

alter table alefdw.dim_guardian add column guardian_invitation_status INT;
alter table alefdw_stage.rel_guardian add column guardian_invitation_status INT;
DROP table alefdw.dim_guardian_invitation;
DROP table alefdw_stage.rel_guardian_invitation;


alter table alefdw_stage.staging_learning_experience
  add column fle_adt_level VARCHAR(20);

alter table alefdw.fact_learning_experience
  add column fle_adt_level VARCHAR(20);

--DROP AFTER DATA MIGRATION ONLY
drop table alefdw_stage.rel_principal;
drop table alefdw_stage.rel_tdc;
drop table alefdw.dim_principal;
drop table alefdw.dim_tdc;
 --DROP AFTER DATA MIGRATION ONLY
CREATE TABLE alefdw_stage.rel_admin
(
  rel_admin_id BIGINT IDENTITY(1,1),
  admin_created_time TIMESTAMP,
  admin_updated_time TIMESTAMP,
  admin_deleted_time TIMESTAMP,
  admin_dw_created_time TIMESTAMP,
  admin_dw_updated_time TIMESTAMP,
  admin_active_until TIMESTAMP,
  admin_status INT,

  admin_uuid VARCHAR(36),
  admin_avatar VARCHAR(100),
  admin_onboarded BOOLEAN,
  school_uuid VARCHAR(36),
  role_uuid VARCHAR(50),
  admin_expirable BOOLEAN
)
compound sortkey(admin_uuid);

CREATE TABLE alefdw.dim_admin
(
  rel_admin_dw_id BIGINT IDENTITY(1,1),
  admin_created_time TIMESTAMP,
  admin_updated_time TIMESTAMP,
  admin_deleted_time TIMESTAMP,
  admin_dw_created_time TIMESTAMP,
  admin_dw_updated_time TIMESTAMP,
  admin_active_until TIMESTAMP,
  admin_status INT,

  admin_id VARCHAR(36),
  admin_dw_id BIGINT,
  admin_avatar VARCHAR(100),
  admin_onboarded BOOLEAN,
  admin_school_dw_id BIGINT,
  admin_role_dw_id BIGINT,
  admin_expirable BOOLEAN
)
distkey(admin_school_dw_id)
compound sortkey(admin_dw_id);

CREATE TABLE alefdw_stage.staging_experience_submitted
(
  fes_staging_id BIGINT IDENTITY(1,1),
  fes_created_time TIMESTAMP,
  fes_dw_created_time TIMESTAMP,
  fes_date_dw_id BIGINT,
  fes_id VARCHAR(36),
  exp_uuid VARCHAR(36),
  fes_ls_id VARCHAR(36),
  fes_step_id VARCHAR(36),
  lo_uuid VARCHAR(36),
  student_uuid VARCHAR(36),
  section_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  subject_uuid VARCHAR(36),
  grade_uuid VARCHAR(36),
  tenant_uuid VARCHAR(36),
  school_uuid VARCHAR(36),
  lp_uuid VARCHAR(36),
  fes_instructional_plan_id VARCHAR(36),
  fes_content_package_id VARCHAR(36),
  fes_content_title VARCHAR(100),
  fes_content_type VARCHAR(100),
  fes_start_time TIMESTAMP,
  fes_lesson_type VARCHAR(50),
  fes_is_retry boolean,
  fes_outside_of_school boolean,
  fes_attempt INT,
  fes_academic_period_order INT,
  academic_year_uuid VARCHAR(36),
  fes_content_academic_year INT,
  fes_lesson_category VARCHAR(100),
  fes_suid VARCHAR(36)
)
sortkey(school_uuid, grade_uuid, class_uuid);

CREATE TABLE alefdw.fact_experience_submitted
(
  fes_dw_id BIGINT IDENTITY(1,1),
  fes_created_time TIMESTAMP,
  fes_dw_created_time TIMESTAMP,
  fes_date_dw_id BIGINT,
  fes_id VARCHAR(36),
  fes_exp_dw_id BIGINT,
  fes_ls_dw_id BIGINT,
  fes_step_id VARCHAR(36),
  fes_lo_dw_id BIGINT,
  fes_student_dw_id BIGINT,
  fes_section_dw_id BIGINT,
  fes_class_dw_id BIGINT,
  fes_subject_dw_id BIGINT,
  fes_grade_dw_id BIGINT,
  fes_tenant_dw_id BIGINT,
  fes_school_dw_id BIGINT,
  fes_lp_dw_id BIGINT,
  fes_instructional_plan_id VARCHAR(36),
  fes_content_package_id VARCHAR(36),
  fes_content_title VARCHAR(100),
  fes_content_type VARCHAR(100),
  fes_start_time TIMESTAMP,
  fes_lesson_type VARCHAR(50),
  fes_is_retry boolean,
  fes_outside_of_school boolean,
  fes_attempt INT,
  fes_academic_period_order INT,
  fes_academic_year_dw_id BIGINT,
  fes_content_academic_year INT,
  fes_lesson_category VARCHAR(100),
  fes_suid VARCHAR(36)
)
distkey(fes_exp_dw_id)
compound sortkey(fes_date_dw_id,fes_school_dw_id, fes_grade_dw_id, fes_class_dw_id, fes_student_dw_id);

alter table alefdw.dim_learning_objective drop column lo_theme_id ;

------------------------------------------------------------ * ---------------------------------------------------------

-- DDL for step instance
CREATE TABLE alefdw.dim_step_instance
(
  step_instance_dw_id BIGINT IDENTITY(1,1),
  step_instance_lo_id VARCHAR(36),
  step_instance_created_time TIMESTAMP,
  step_instance_updated_time TIMESTAMP,
  step_instance_dw_created_time TIMESTAMP,
  step_instance_dw_updated_time TIMESTAMP,
  step_instance_status INT,
  step_instance_step_id INT,
  step_instance_step_uuid VARCHAR(36),
  step_instance_lo_ccl_id BIGINT,
  step_instance_id VARCHAR(36),
  step_instance_attach_status INT,
  step_instance_type INT,
  step_instance_pool_name VARCHAR(255),
  step_instance_difficulty_level VARCHAR(50),
  step_instance_resource_type VARCHAR(50),
  step_instance_questions INT
)
  diststyle all
  sortkey(step_instance_dw_id);

alter table alefdw.dim_learning_objective_association drop column lo_association_step_id;
alter table alefdw.dim_learning_objective_association add column lo_association_derived boolean;

CREATE TABLE alefdw_stage.staging_adt_next_question
(
fanq_staging_id BIGINT IDENTITY(1,1),
fanq_created_time TIMESTAMP,
fanq_dw_created_time TIMESTAMP,
fanq_date_dw_id BIGINT,
fanq_id VARCHAR(36),
fle_ls_uuid VARCHAR(36),
student_uuid VARCHAR(36),
fanq_question_pool_id VARCHAR(36),
tenant_uuid VARCHAR(36),
fanq_response BOOLEAN,
fanq_proficiency DOUBLE PRECISION,
fanq_see DOUBLE PRECISION,
fanq_next_question_id VARCHAR(36),
fanq_time_spent DOUBLE PRECISION
)
sortkey(fanq_date_dw_id);

CREATE TABLE alefdw.fact_adt_next_question
(
fanq_dw_id BIGINT IDENTITY(1,1),
fanq_created_time TIMESTAMP,
fanq_dw_created_time TIMESTAMP,
fanq_date_dw_id BIGINT,
fanq_id VARCHAR(36),
fanq_fle_ls_dw_id VARCHAR(36),
fanq_student_dw_id VARCHAR(36),
fanq_question_pool_id VARCHAR(36),
fanq_tenant_dw_id VARCHAR(36),
fanq_response BOOLEAN,
fanq_proficiency DOUBLE PRECISION,
fanq_see DOUBLE PRECISION,
fanq_next_question_id VARCHAR(36),
fanq_time_spent DOUBLE PRECISION
)
distkey(fanq_fle_ls_dw_id)
compound sortkey(fanq_date_dw_id,fanq_student_dw_id,fanq_question_pool_id);

CREATE TABLE alefdw_stage.staging_adt_student_report(
fasr_staging_id BIGINT IDENTITY(1,1),
fasr_created_time TIMESTAMP,
fasr_dw_created_time TIMESTAMP,
fasr_date_dw_id BIGINT,
tenant_uuid varchar(36),
student_uuid varchar(36),
fasr_question_pool_id varchar(36),
fle_ls_uuid varchar(36),
fasr_id varchar(36),
fasr_scaler_version varchar(20),
fasr_final_see DOUBLE PRECISION,
fasr_final_score DOUBLE PRECISION,
fasr_final_proficiency DOUBLE PRECISION,
fasr_final_cefr varchar(20),
fasr_total_time_spent DOUBLE PRECISION
);

CREATE TABLE alefdw.fact_adt_student_report(
fasr_dw_id BIGINT IDENTITY(1,1),
fasr_created_time TIMESTAMP,
fasr_dw_created_time TIMESTAMP,
fasr_date_dw_id BIGINT,
fasr_tenant_dw_id bigint,
fasr_student_dw_id bigint,
fasr_question_pool_id varchar(36),
fasr_fle_ls_dw_id bigint,
fasr_id varchar(36),
fasr_scaler_version varchar(20),
fasr_final_see DOUBLE PRECISION,
fasr_final_score DOUBLE PRECISION,
fasr_final_proficiency DOUBLE PRECISION,
fasr_final_cefr varchar(20),
fasr_total_time_spent DOUBLE PRECISION
)
distkey(fasr_dw_id)
compound sortkey(fasr_date_dw_id, fasr_tenant_dw_id, fasr_student_dw_id);

alter table alefdw.dim_content_student_association add column content_student_association_assigned_by varchar(36);
alter table alefdw.dim_content_student_association add column content_student_association_type smallint;
alter table alefdw.dim_content_student_association add column content_student_association_lo_id varchar(36);
alter table alefdw.dim_content_student_association add column content_student_association_class_id varchar(36);

-- Migration from content to step
alter table alefdw.fact_learning_experience add column fle_step_id varchar(36);
--run update--redshift_updates.sql
alter table alefdw.fact_learning_experience drop column fle_content_id;
alter table alefdw_stage.staging_learning_experience rename column fle_content_id to fle_step_id;


alter table alefdw.fact_practice add column practice_item_step_id varchar(36);
--run update--redshift_updates.sql
alter table alefdw.fact_practice drop column practice_item_content_id;
alter table alefdw_stage.staging_practice rename column practice_item_content_id to practice_item_step_id;


alter table alefdw.fact_practice_session add column practice_session_item_step_id varchar(36);
--run update--redshift_updates.sql
alter table alefdw.fact_practice_session drop column practice_session_item_content_uuid;
alter table alefdw_stage.staging_practice_session rename column practice_session_item_content_uuid to practice_session_item_step_id;


alter table alefdw.dim_content_student_association add column content_student_association_step_id varchar(36);
--run update--redshift_updates.sql
alter table alefdw.dim_content_student_association drop column content_student_association_content_id;

alter table alefdw.dim_school add column school_organisation_dw_id integer;
-- alter table alefdw.dim_school drop column school_organisation; --(to be used later after BI changes)

create table alefdw_stage.temp_org as select * from alefdw.dim_organisation;
drop table alefdw.dim_organisation;
create table if not exists alefdw.dim_organisation
(
 organisation_dw_id BIGINT IDENTITY(1,1),
 organisation_id varchar(36),
 organisation_name varchar(20)
)
diststyle all
sortkey(organisation_id);
insert into alefdw.dim_organisation (organisation_id, organisation_name) (select organisation_id, organisation_name from alefdw_stage.temp_org);
drop table alefdw_stage.temp_org;
alter table alefdw.dim_organisation add column organisation_dw_id BIGINT;

alter table alefdw.dim_instructional_plan add column instructional_plan_organisation_dw_id integer;
alter table alefdw_stage.rel_instructional_plan add column organisation_uuid varchar(36);
alter table alefdw.dim_term add column term_organisation_dw_id integer;
-- alter table alefdw.dim_term drop column term_organisation_id; (to be used later after BI changes

create table if not exists alefdw.dim_holiday(
   holiday_dw_id BIGINT IDENTITY(1,1),
   holiday_date date,
   holiday_name VARCHAR(100),
   holiday_country VARCHAR(50)
)
 diststyle all
 compound sortkey(holiday_dw_id);

insert into alefdw.dim_holiday (holiday_date, holiday_name,holiday_country) VALUES
('2020-01-01','New Year''s Day','UAE'),
('2020-05-22','Eid al-Fitr','UAE'),
('2020-05-24','Eid al-Fitr','UAE'),
('2020-05-25','Eid al-Fitr','UAE'),
('2020-05-26','Eid al-Fitr','UAE'),
('2020-07-30','Afrah Day','UAE'),
('2020-07-31','Eid Al Adha','UAE'),
('2020-08-01','Eid Al Adha','UAE'),
('2020-08-02','Eid Al Adha','UAE'),
('2020-08-23','Islamic New Year','UAE'),
('2020-10-29','Prophet Muhammad''s birthday','UAE'),
('2020-12-01','Commemoration Day','UAE'),
('2020-12-02','National Day','UAE'),
('2020-12-03','National Day','UAE'),
('2021-01-01','New Year''s Day','UAE'),
('2021-05-11','Eid al-Fitr','UAE'),
('2021-12-05','Eid al-Fitr','UAE'),
('2021-05-13','Eid al-Fitr','UAE'),
('2021-05-14','Eid al-Fitr','UAE'),
('2021-05-15','Eid al-Fitr','UAE'),
('2021-07-19','Arafat day','UAE'),
('2021-07-20','Eid Al Adha','UAE'),
('2021-07-21','Eid Al Adha','UAE'),
('2021-07-22','Eid Al Adha','UAE'),
('2021-08-12','Islamic New Year','UAE'),
('2021-10-21','Prophet Muhammad''s birthday','UAE'),
('2021-12-01','Commemoration Day','UAE'),
('2021-12-02','National Day','UAE'),
('2021-12-03','National Day','UAE');

alter table alefdw.dim_step_instance add column step_instance_pool_id VARCHAR(36);

alter table alefdw_stage.staging_experience_submitted add column fes_abbreviation Varchar(100);
alter table alefdw_stage.staging_experience_submitted add column fes_activity_type Varchar(100);
alter table alefdw_stage.staging_experience_submitted add column fes_activity_component_type Varchar(100);
alter table alefdw_stage.staging_experience_submitted add column fes_completion_node BOOLEAN;
alter table alefdw_stage.staging_experience_submitted add column fes_main_component BOOLEAN;
alter table alefdw_stage.staging_experience_submitted add column fes_exit_ticket BOOLEAN;

alter table alefdw.fact_experience_submitted add column fes_abbreviation Varchar(100);
alter table alefdw.fact_experience_submitted add column fes_activity_type Varchar(100);
alter table alefdw.fact_experience_submitted add column fes_activity_component_type Varchar(100);
alter table alefdw.fact_experience_submitted add column fes_completion_node BOOLEAN;
alter table alefdw.fact_experience_submitted add column fes_main_component BOOLEAN;
alter table alefdw.fact_experience_submitted add column fes_exit_ticket BOOLEAN;


alter table alefdw_stage.staging_learning_experience add column fle_abbreviation Varchar(100);
alter table alefdw_stage.staging_learning_experience add column fle_activity_template_id Varchar(100);
alter table alefdw_stage.staging_learning_experience add column fle_activity_type Varchar(100);
alter table alefdw_stage.staging_learning_experience add column fle_activity_component_type Varchar(100);
alter table alefdw_stage.staging_learning_experience add column fle_exit_ticket BOOLEAN;
alter table alefdw_stage.staging_learning_experience add column fle_main_component BOOLEAN;
alter table alefdw_stage.staging_learning_experience add column fle_completion_node BOOLEAN;

alter table alefdw.fact_learning_experience add column fle_abbreviation Varchar(100);
alter table alefdw.fact_learning_experience add column fle_activity_template_id Varchar(100);
alter table alefdw.fact_learning_experience add column fle_activity_type Varchar(100);
alter table alefdw.fact_learning_experience add column fle_activity_component_type Varchar(100);
alter table alefdw.fact_learning_experience add column fle_exit_ticket BOOLEAN;
alter table alefdw.fact_learning_experience add column fle_main_component BOOLEAN;
alter table alefdw.fact_learning_experience add column fle_completion_node BOOLEAN;


create table alefdw_stage.staging_assignment_submission
(
    assignment_submission_staging_id                BIGINT IDENTITY (1,1),
    assignment_submission_id                           VARCHAR(36),
    assignment_submission_assignment_id                VARCHAR(36),
    assignment_submission_referrer_id                  VARCHAR(36),
    assignment_submission_type                         VARCHAR(100),
    assignment_submission_updated_on                   TIMESTAMP,
    assignment_submission_returned_on                  TIMESTAMP,
    assignment_submission_submitted_on                 TIMESTAMP,
    assignment_submission_graded_on                    TIMESTAMP,
    assignment_submission_evaluated_on                 TIMESTAMP,
    assignment_submission_status                       VARCHAR(20),
    assignment_submission_student_attachment_file_name VARCHAR(200),
    assignment_submission_student_attachment_path      VARCHAR(200),
    assignment_submission_teacher_attachment_path      VARCHAR(200),
    assignment_submission_teacher_attachment_file_name VARCHAR(200),
    assignment_submission_teacher_score                FLOAT,
    assignment_submission_date_dw_id                   VARCHAR(36),
    assignment_submission_created_time                 TIMESTAMP,
    assignment_submission_dw_created_time              TIMESTAMP,

    assignment_submission_has_teacher_comment          BOOLEAN,
    assignment_submission_has_student_comment          BOOLEAN,

    assignment_submission_assignment_instance_id    VARCHAR(36),
    assignment_submission_student_id                VARCHAR(36),
    assignment_submission_teacher_id                VARCHAR(36),
    assignment_submission_tenant_id                 VARCHAR(36),
    eventdate                 DATE
)
;

create table alefdw.fact_assignment_submission
(
    assignment_submission_dw_id                        BIGINT IDENTITY (1,1),
    assignment_submission_id                           VARCHAR(36),
    assignment_submission_assignment_id                VARCHAR(36),
    assignment_submission_referrer_id                  VARCHAR(36),
    assignment_submission_type                         VARCHAR(100),
    assignment_submission_updated_on                   TIMESTAMP,
    assignment_submission_returned_on                  TIMESTAMP,
    assignment_submission_submitted_on                 TIMESTAMP,
    assignment_submission_graded_on                    TIMESTAMP,
    assignment_submission_evaluated_on                 TIMESTAMP,
    assignment_submission_status                       VARCHAR(20),
    assignment_submission_student_attachment_file_name VARCHAR(200),
    assignment_submission_student_attachment_path      VARCHAR(200),
    assignment_submission_teacher_attachment_path      VARCHAR(200),
    assignment_submission_teacher_attachment_file_name VARCHAR(200),
    assignment_submission_teacher_score                FLOAT,
    assignment_submission_date_dw_id                   VARCHAR(36),
    assignment_submission_created_time                 TIMESTAMP,
    assignment_submission_dw_created_time              TIMESTAMP,

    assignment_submission_has_teacher_comment          BOOLEAN,
    assignment_submission_has_student_comment          BOOLEAN,

    assignment_submission_assignment_instance_id       VARCHAR(36),
    assignment_submission_student_dw_id                   VARCHAR(36),
    assignment_submission_teacher_dw_id                   VARCHAR(36),
    assignment_submission_tenant_dw_id                    VARCHAR(36),
    eventdate                                          DATE
)
distkey ( assignment_submission_assignment_instance_id )
sortkey (assignment_submission_tenant_dw_id, assignment_submission_student_dw_id, assignment_submission_created_time)
;

alter table alefdw.dim_teacher drop column teacher_source_identifier;
alter table alefdw_stage.rel_teacher drop column teacher_source_identifier;

create table if not exists alefdw_stage.rel_assignment
(
	rel_assignment_id BIGINT IDENTITY(1,1),
	assignment_created_time TIMESTAMP,
	assignment_updated_time TIMESTAMP,
	assignment_deleted_time TIMESTAMP,
	assignment_dw_created_time TIMESTAMP,
	assignment_dw_updated_time TIMESTAMP,
	assignment_id VARCHAR(36),
	assignment_title VARCHAR(100),
	assignment_description VARCHAR(250),
	assignment_max_score NUMERIC(10,4),
	assignment_attachment_file_id VARCHAR(100),
	assignment_attachment_file_name VARCHAR(100),
	assignment_attachment_path VARCHAR(200),
	assignment_graded BOOLEAN,
	assignment_allow_submission BOOLEAN,
	assignment_language VARCHAR(36),
	assignment_status INTEGER,
    assignment_is_gradeable boolean,
    assignment_assignment_status VARCHAR(36),
    assignment_created_by VARCHAR(36),
    assignment_updated_by VARCHAR(36),
    assignment_published_on TIMESTAMP,
    assignment_attachment_required VARCHAR(36),
    assignment_comment_required VARCHAR(36),
    assignment_type VARCHAR(36),
    assignment_metadata_author VARCHAR(36),
    assignment_metadata_is_sa BOOLEAN,
    assignment_metadata_authored_date VARCHAR(36),
    assignment_metadata_language VARCHAR(36),
    assignment_metadata_format_type VARCHAR(36),
    assignment_metadata_lexile_level VARCHAR(36),
    assignment_metadata_difficulty_level VARCHAR(36),
    assignment_metadata_resource_type VARCHAR(36),
    assignment_metadata_knowledge_dimensions VARCHAR(36),
    assignment_school_id VARCHAR(36),
    assignment_tenant_id VARCHAR(36)
);

create table if not exists alefdw.dim_assignment
(
	assignment_dw_id BIGINT IDENTITY(1,1),
	assignment_created_time TIMESTAMP,
	assignment_updated_time TIMESTAMP,
	assignment_deleted_time TIMESTAMP,
	assignment_dw_created_time TIMESTAMP,
	assignment_dw_updated_time TIMESTAMP,
	assignment_id VARCHAR(36),
	assignment_title VARCHAR(100),
	assignment_description VARCHAR(250),
	assignment_max_score NUMERIC(10,4),
	assignment_attachment_file_id VARCHAR(100),
	assignment_attachment_file_name VARCHAR(100),
	assignment_attachment_path VARCHAR(200),
	assignment_graded BOOLEAN,
	assignment_allow_submission BOOLEAN,
	assignment_language VARCHAR(36),
	assignment_status INTEGER,
    assignment_is_gradeable boolean,
    assignment_assignment_status VARCHAR(36),
    assignment_created_by VARCHAR(36),
    assignment_updated_by VARCHAR(36),
    assignment_published_on TIMESTAMP,
    assignment_attachment_required VARCHAR(36),
    assignment_comment_required VARCHAR(36),
    assignment_type VARCHAR(36),
    assignment_metadata_author VARCHAR(36),
    assignment_metadata_is_sa BOOLEAN,
    assignment_metadata_authored_date VARCHAR(36),
    assignment_metadata_language VARCHAR(36),
    assignment_metadata_format_type VARCHAR(36),
    assignment_metadata_lexile_level VARCHAR(36),
    assignment_metadata_difficulty_level VARCHAR(36),
    assignment_metadata_resource_type VARCHAR(36),
    assignment_metadata_knowledge_dimensions VARCHAR(36),
    assignment_school_dw_id VARCHAR(36),
    assignment_tenant_dw_id VARCHAR(36)
);

alter table alefdw.dim_outcome add column outcome_description VARCHAR(1500);

alter table alefdw.dim_content add column content_organisation VARCHAR(50);
alter table alefdw.dim_curriculum add column curr_organisation VARCHAR(50);

alter table alefdw.dim_assignment_instance add column assignment_instance_active_until TIMESTAMP;
alter table alefdw_stage.rel_assignment_instance add column assignment_instance_active_until TIMESTAMP;

alter table alefdw.dim_learning_objective add column lo_organisation VARCHAR(50);
alter table alefdw.dim_template add column template_organisation VARCHAR(50);

alter table alefdw.dim_content add column content_learning_resource_types VARCHAR(200);
alter table alefdw.dim_content add column content_cognitive_dimensions VARCHAR(200);
alter table alefdw.dim_outcome add column outcome_description VARCHAR(1500);

--alter table alefdw.dim_instructional_plan drop column instructional_plan_organisation_id; (to be used later after BI changes)
--alter table alefdw_stage.rel_instructional_plan drop column instructional_plan_organisation_id; (to be used later after BI changes)

CREATE TABLE alefdw_stage.rel_lesson_assignment
(
  rel_lesson_assignment_id BIGINT IDENTITY(1,1),
  lesson_assignment_created_time TIMESTAMP,
  lesson_assignment_updated_time TIMESTAMP,
  lesson_assignment_dw_created_time TIMESTAMP,
  lesson_assignment_dw_updated_time TIMESTAMP,
  lesson_assignment_status INT,
  student_uuid VARCHAR(36),
  lo_uuid VARCHAR(36),
  class_uuid VARCHAR(36),
  teacher_uuid VARCHAR(36),
  lesson_assignment_assign_status INT,
  lesson_assignment_type INT
)
diststyle all
sortkey(rel_lesson_assignment_id);

CREATE TABLE alefdw.dim_lesson_assignment
(
  lesson_assignment_dw_id BIGINT IDENTITY(1,1),
  lesson_assignment_created_time TIMESTAMP,
  lesson_assignment_updated_time TIMESTAMP,
  lesson_assignment_dw_created_time TIMESTAMP,
  lesson_assignment_dw_updated_time TIMESTAMP,
  lesson_assignment_status INT,
  lesson_assignment_student_dw_id BIGINT,
  lesson_assignment_lo_dw_id BIGINT,
  lesson_assignment_class_dw_id BIGINT,
  lesson_assignment_teacher_dw_id INT,
  lesson_assignment_assign_status INT,
  lesson_assignment_type INT
)
sortkey(lesson_assignment_dw_id);


create table alefdw.dim_activity_template
(
  at_dw_id BIGINT IDENTITY (1,1),
  at_uuid VARCHAR(36),
  at_name VARCHAR(256),
  at_description VARCHAR(4096),
  at_status INT, -- 0 - inactive, 1 - active
  at_activity_type VARCHAR(50),
  at_created_time TIMESTAMP,
  at_updated_time TIMESTAMP,
  at_dw_created_time TIMESTAMP,
  at_dw_updated_time TIMESTAMP,
  at_publisher_id BIGINT,
  at_publisher_name VARCHAR(255),
  at_published_date TIMESTAMP,
  at_component_uuid VARCHAR(36),
  at_order INT,
  at_component_name VARCHAR(256),
  at_component_type INT, -- 1 - CONTENT || 5 - ASSESSMENT || 4 - ASSIGNMENT || 6 - KEY_TERM
  at_abbreviation VARCHAR(50),
  at_icon VARCHAR(256),
  at_max_repeat INT,
  at_exit_ticket BOOLEAN,
  at_completion_node BOOLEAN,
  at_always_enabled BOOLEAN,
  at_passing_score INT,
  at_assessments_attempts INT,
  at_question_attempts_hints BOOLEAN,
  at_question_attempts INT,
  at_release_condition VARCHAR(36),
  at_section_type INT, -- 1 - MAIN_COMPONENT || 2 - SUPPORTING_COMPONENT || 3 - SIDE_COMPONENT
  at_release_component VARCHAR(36),
  at_min INT,
  at_max INT
)
  diststyle all
  sortkey(at_dw_id);

alter table alefdw.dim_content_student_association add column content_student_association_content_type VARCHAR(50);

create table if not exists alefdw.dim_assignment_instance_student
(
    ais_dw_id                 BIGINT IDENTITY(1, 1),
    ais_created_time          TIMESTAMP,
    ais_dw_created_time       TIMESTAMP,
    ais_updated_time          TIMESTAMP,
    ais_dw_updated_time       TIMESTAMP,
    ais_deleted_time          TIMESTAMP,
    ais_status                INTEGER,
    ais_instance_dw_id        BIGINT,
    ais_student_dw_id         BIGINT
) sortkey (ais_instance_dw_id, ais_student_dw_id);

create table if not exists alefdw_stage.rel_assignment_instance_student
(
    rel_ais_id                BIGINT IDENTITY(1,1),
    ais_created_time          TIMESTAMP,
    ais_dw_created_time       TIMESTAMP,
    ais_updated_time          TIMESTAMP,
    ais_dw_updated_time       TIMESTAMP,
    ais_deleted_time          TIMESTAMP,
    ais_status                INTEGER,
    ais_instance_id           VARCHAR(36),
    ais_student_id            VARCHAR(36)
);

CREATE TABLE alefdw.dim_teacher_feedback_thread
(
    tft_dw_id              BIGINT IDENTITY (1,1),
    tft_status             SMALLINT,
    tft_created_time       TIMESTAMP,
    tft_dw_created_time    TIMESTAMP,
    tft_deleted_time       TIMESTAMP,
    tft_updated_time       TIMESTAMP,
    tft_dw_updated_time    TIMESTAMP,
    tft_thread_id          VARCHAR(36),
    tft_actor_type         SMALLINT,
    tft_guardian_dw_id     BIGINT,
    tft_message_id         VARCHAR(36),
    tft_response_enabled   BOOLEAN,
    tft_feedback_type      SMALLINT,
    tft_teacher_dw_id      BIGINT,
    tft_student_dw_id      BIGINT,
    tft_class_dw_id        BIGINT,
    tft_is_read            BOOLEAN,
    tft_event_subject      SMALLINT,
    tft_is_first_of_thread BOOLEAN
) sortkey (tft_teacher_dw_id, tft_class_dw_id, tft_guardian_dw_id, tft_student_dw_id);

CREATE TABLE alefdw_stage.rel_teacher_feedback_thread
(
    rel_tft_id             BIGINT IDENTITY (1,1),
    tft_status             SMALLINT,
    tft_created_time       TIMESTAMP,
    tft_dw_created_time    TIMESTAMP,
    tft_deleted_time       TIMESTAMP,
    tft_updated_time       TIMESTAMP,
    tft_dw_updated_time    TIMESTAMP,
    tft_thread_id          VARCHAR(36),
    tft_actor_type         SMALLINT,
    tft_guardian_id        VARCHAR(36),
    tft_message_id         VARCHAR(36),
    tft_response_enabled   BOOLEAN,
    tft_feedback_type      SMALLINT,
    tft_teacher_id         VARCHAR(36),
    tft_student_id         VARCHAR(36),
    tft_class_id           VARCHAR(36),
    tft_is_read            BOOLEAN,
    tft_event_subject      SMALLINT,
    tft_is_first_of_thread BOOLEAN
);

--- REMOVE STORING OF STUDENTS IN ASSIGNMENT INSTANCE --
alter table alefdw.dim_assignment_instance alter sortkey(assignment_instance_grade_dw_id, assignment_instance_class_dw_id);
alter table alefdw.dim_assignment_instance drop column assignment_instance_student_dw_id;

alter table alefdw.dim_assignment_instance drop column assignment_instance_student_id;
alter table alefdw_stage.rel_assignment_instance drop column assignment_instance_student_id;

alter table alefdw.dim_assignment_instance drop column assignment_instance_student_status;
alter table alefdw_stage.rel_assignment_instance drop column assignment_instance_student_status;

alter table alefdw_stage.rel_assignment_instance drop column assignment_instance_active_until;

-- Addition of new columns to instruction plan events
alter table alefdw_stage.rel_instructional_plan add column instructional_plan_item_instructor_led boolean;
alter table alefdw.dim_instructional_plan add column instructional_plan_item_instructor_led boolean;

alter table alefdw_stage.rel_instructional_plan add column instructional_plan_item_default_locked boolean;
alter table alefdw.dim_instructional_plan add column instructional_plan_item_default_locked boolean;

create table alefdw.dim_cx_role(
  role_dw_id BIGINT IDENTITY (1,1),
  role_id INT,
  role_title VARCHAR(20),
  role_status INT,
  role_created_time TIMESTAMP,
  role_dw_created_time TIMESTAMP,
  role_updated_time TIMESTAMP,
  role_dw_updated_time TIMESTAMP,
  role_deleted_time TIMESTAMP
)
  diststyle all
  sortkey(role_dw_id);


alter table alefdw.dim_learning_objective add column lo_language VARCHAR(50);
alter table alefdw.dim_learning_objective add column lo_template_uuid VARCHAR(36);
alter table alefdw.dim_step_instance add column step_instance_template_uuid VARCHAR(36);
alter table alefdw.dim_step_instance add column step_instance_title VARCHAR(255);
alter table alefdw.dim_step_instance add column step_instance_location VARCHAR(255);
alter table alefdw.dim_step_instance add column step_instance_media_type VARCHAR(50);

alter table alefdw.dim_class add column class_curriculum_instructional_plan_id VARCHAR(36);


-- CX DDL Changes
-- Users Table --
CREATE TABLE alefdw.dim_cx_user
(
    cx_user_dw_id BIGINT,
    cx_user_cx_id BIGINT,
    cx_user_id VARCHAR(36),
    cx_user_cx_status INT,
    cx_user_status INT,
    cx_user_subject VARCHAR(25),
    cx_user_role_dw_id BIGINT,
    cx_user_created_time TIMESTAMP,
    cx_user_dw_created_time TIMESTAMP,
    cx_user_updated_time TIMESTAMP,
    cx_user_dw_updated_time TIMESTAMP,
    cx_user_deleted_time TIMESTAMP
)
diststyle all
compound sortkey(cx_user_dw_id);

-- Schools Table --
alter table alefdw.dim_school add column school_cx_id BIGINT;
alter table alefdw.dim_school add column school_cx_name_ar VARCHAR(200);
alter table alefdw.dim_school add column school_cx_zone VARCHAR(50);
alter table alefdw.dim_school add column school_cx_cluster VARCHAR(50);
alter table alefdw.dim_school add column school_cx_batch INT;
alter table alefdw.dim_school add column school_cx_status INT;
alter table alefdw.dim_school add column school_cx_report INT;
alter table alefdw.dim_school add column school_cx_last_updated TIMESTAMP;

-- CX Training Table --
CREATE TABLE alefdw.fact_cx_training
(
    fct_dw_id BIGINT IDENTITY(1,1),
    fct_id BIGINT,
    fct_plc_code_dw_id BIGINT,
    fct_trainee_dw_id BIGINT,
    fct_training_status INT,
    fct_subscription_date TIMESTAMP,
    fct_completion_date TIMESTAMP,
    fct_created_time TIMESTAMP,
    fct_dw_created_time TIMESTAMP
)
distkey(fct_plc_code_dw_id)
sortkey(fct_dw_id);

-- CX Maturity --
CREATE TABLE alefdw.dim_cx_maturity
(
    cx_maturity_dw_id BIGINT IDENTITY(1,1),
    cx_maturity_id BIGINT,
    cx_maturity_evaluation_id VARCHAR(15),
    cx_maturity_tdc_dw_id BIGINT,
    cx_maturity_sch_dw_id BIGINT,
    cx_maturity_acc_year_dw_id BIGINT,
    cx_maturity_trimester INT,
    cx_maturity_submit VARCHAR(5),
    cx_maturity_approved VARCHAR(5),
    cx_maturity_submit_date TIMESTAMP,
    cx_maturity_principal_factor FLOAT,
    cx_maturity_teacher_factor FLOAT,
    cx_maturity_student_factor FLOAT,
    cx_maturity_parent_factor FLOAT,
    cx_maturity_total_avg FLOAT,
    cx_maturity_status INT,
    cx_maturity_created_time TIMESTAMP,
    cx_maturity_dw_created_time TIMESTAMP,
    cx_maturity_updated_time TIMESTAMP,
    cx_maturity_dw_updated_time TIMESTAMP,
    cx_maturity_deleted_time TIMESTAMP
)
    distkey(cx_maturity_acc_year_dw_id)
    sortkey(cx_maturity_dw_id);

-- CX Indicator Maturity --
CREATE TABLE alefdw.fact_cx_maturity_indicator
(
    fcmi_dw_id BIGINT IDENTITY(1,1),
    fcmi_id BIGINT,
    fcmi_maturity_dw_id BIGINT,
    fcmi_indicator_id VARCHAR(25),
    fcmi_indicator_value INT,
    fcmi_created_time TIMESTAMP,
    fcmi_dw_created_time TIMESTAMP
)
    distkey(fcmi_indicator_id)
    sortkey(fcmi_dw_id);

-- CX Observation --
CREATE TABLE alefdw.dim_cx_observation
(
    cx_observation_dw_id BIGINT IDENTITY(1,1),
    cx_observation_observation_id BIGINT,
    cx_observation_observed_dw_id BIGINT,
    cx_observation_observer_dw_id BIGINT,
    cx_observation_observation_timing VARCHAR(25),
    cx_observation_observed_grade INT,
    cx_observation_observed_period INT,
    cx_observation_observation_score INT,
    cx_observation_joint_observer_dw_id BIGINT,
    cx_observation_observation_submit_time TIMESTAMP,
    cx_observation_observation_sent_time TIMESTAMP,
    cx_observation_observation_type VARCHAR(25),
    cx_observation_coaching_closing_time TIMESTAMP,
    cx_observation_coach_dw_id BIGINT,
    cx_observation_observation_center_dw_id BIGINT,
    cx_observation_observation_archived VARCHAR(60),
    cx_observation_status INT,
    cx_observation_created_time TIMESTAMP,
    cx_observation_dw_created_time TIMESTAMP,
    cx_observation_updated_time TIMESTAMP,
    cx_observation_dw_updated_time TIMESTAMP,
    cx_observation_deleted_time TIMESTAMP
)
    distkey(cx_observation_observation_id)
    sortkey(cx_observation_dw_id);

-- CX Observation Indicator --
CREATE TABLE alefdw.fact_cx_observation_indicator
(
    fcoi_dw_id BIGINT IDENTITY(1,1),
    fcoi_id BIGINT,
    fcoi_observation_dw_id BIGINT,
    fcoi_indicator_id BIGINT,
    fcoi_indicator_value FLOAT,
    fcoi_coaching_time TIMESTAMP,
    fcoi_created_time TIMESTAMP,
    fcoi_dw_created_time TIMESTAMP
)
    distkey(fcoi_indicator_id)
    sortkey(fcoi_dw_id);

-- CX User Log --
CREATE TABLE alefdw.fact_cx_user_log
(
    fcul_dw_id BIGINT IDENTITY(1,1),
    fcul_id BIGINT,
    fcul_actor_dw_id BIGINT,
    fcul_effected_user_dw_id BIGINT,
    fcul_action VARCHAR(50),
    fcul_time TIMESTAMP,
    fcul_created_time TIMESTAMP,
    fcul_dw_created_time TIMESTAMP
)
    distkey(fcul_id)
    sortkey(fcul_dw_id);

-- Academic year add completed flag --
alter table alefdw.dim_academic_year add column academic_year_is_roll_over_completed BOOLEAN DEFAULT false;

-- Academic year add completed flag --
alter table alefdw_stage.staging_learning_experience add column fle_is_activity_completed BOOLEAN;
alter table alefdw.fact_learning_experience add column fle_is_activity_completed BOOLEAN;

-- Observation Indicator --
CREATE TABLE alefdw.fact_cx_observation_indicator
(
    fcoi_dw_id BIGINT IDENTITY(1,1),
    fcoi_id BIGINT,
    fcoi_observation_dw_id BIGINT,
    fcoi_indicator_id BIGINT,
    fcoi_indicator_value FLOAT,
    fcoi_coaching_time TIMESTAMP,
    fcoi_created_time TIMESTAMP,
    fcoi_dw_created_time TIMESTAMP
)
sortkey(fcoi_dw_id);

-- dim class --
ALTER TABLE alefdw.dim_class ADD COLUMN class_category_id VARCHAR(36);
ALTER TABLE alefdw.dim_class ADD COLUMN class_active_until TIMESTAMP;


CREATE TABLE alefdw.dim_class_category
(
    class_category_dw_id BIGINT IDENTITY(1,1),
    class_category_id VARCHAR(36),
    class_category_name VARCHAR(50),
    class_category_created_time TIMESTAMP,
    class_category_dw_created_time TIMESTAMP,
    class_category_updated_time TIMESTAMP,
    class_category_dw_updated_time TIMESTAMP
) sortkey(class_category_dw_id);


--******************************************TO BE RELEASED**************************************************************
------- Add REL_CLASS TABLE -------
CREATE TABLE alefdw_stage.rel_dw_id_mappings (class_dw_id bigint identity(1,1), class_id varchar(36));

CREATE TABLE alefdw_stage.rel_class (
                                            rel_class_id bigint identity (1,1),
                                            class_created_time timestamp,
                                            class_updated_time timestamp,
                                            class_deleted_time timestamp,
                                            class_dw_created_time timestamp,
                                            class_dw_updated_time timestamp,
                                            class_status int,
                                            class_id varchar(36),
                                            class_title varchar(255),
                                            class_school_id varchar(36),
                                            class_grade_id varchar(36),
                                            class_section_id varchar(36),
                                            class_academic_year_id varchar(36),
                                            class_gen_subject varchar(255) ,
                                            class_curriculum_id bigint,
                                            class_curriculum_grade_id bigint,
                                            class_curriculum_subject_id bigint,
                                            class_content_academic_year int,
                                            class_tutor_dhabi_enabled boolean,
                                            class_language_direction varchar(25),
                                            class_online boolean,
                                            class_practice boolean,
                                            class_course_status varchar(50),
                                            class_source_id varchar(255),
                                            class_curriculum_instructional_plan_id varchar(36),
                                            class_category_id varchar(36),
                                            class_active_until timestamp
);

-- we dont need to run the following statement. it will be done through script. Refer to redshift_updates.sql for the script
ALTER TABLE alefdw.dim_class ADD COLUMN rel_class_dw_id bigint identity (1,1);

-- instructional_plan add checkpoint support
alter table alefdw_stage.rel_instructional_plan add column instructional_plan_item_type varchar(36);
alter table alefdw.dim_instructional_plan add column instructional_plan_item_type varchar(36);

alter table alefdw_stage.rel_instructional_plan add column instructional_plan_checkpoint_id varchar(36);
alter table alefdw.dim_instructional_plan add column instructional_plan_checkpoint_dw_id bigint;

ALTER TABLE alefdw_stage.rel_instructional_plan RENAME COLUMN instructional_plan_checkpoint_id TO ic_uuid;
ALTER TABLE alefdw.dim_instructional_plan RENAME COLUMN instructional_plan_checkpoint_dw_id TO instructional_plan_item_ic_dw_id;

-- CX PLC_Info Table --
CREATE TABLE alefdw.dim_cx_plc_info
(
    cx_plc_info_dw_id BIGINT IDENTITY(1,1),
    cx_plc_info_id BIGINT,
    cx_plc_info_code VARCHAR(10),
    cx_plc_info_term INT,
    cx_plc_info_acc_year VARCHAR(9),
    cx_plc_info_creation_date TIMESTAMP,
    cx_plc_info_title VARCHAR(500),
    cx_plc_info_audience INT,
    cx_plc_info_status INT,
    cx_plc_info_cx_status INT,
    cx_plc_info_created_time TIMESTAMP,
    cx_plc_info_dw_created_time TIMESTAMP,
    cx_plc_info_updated_time TIMESTAMP,
    cx_plc_info_dw_updated_time TIMESTAMP,
    cx_plc_info_deleted_time TIMESTAMP
)
    diststyle all
compound sortkey(cx_plc_info_dw_id);

alter table alefdw.dim_class_category add column class_category_status INT;

create table alefdw.dim_interim_checkpoint
(
  ic_dw_id           BIGINT IDENTITY (1, 1),
  ic_created_time    TIMESTAMP,
  ic_dw_created_time TIMESTAMP,
  ic_deleted_time    TIMESTAMP,
  ic_dw_deleted_time TIMESTAMP,
  ic_updated_time    TIMESTAMP,
  ic_dw_updated_time TIMESTAMP,
  ic_status          SMALLINT,
  ic_id              VARCHAR(36),
  ic_title           VARCHAR(250),
  ic_language        VARCHAR(50),
  ic_ip_id           VARCHAR(36)
)
  diststyle all
  compound sortkey (ic_dw_id);

create table alefdw_stage.rel_ic_lesson_association
(
  rel_ic_lesson_id          BIGINT IDENTITY (1, 1),
  ic_lesson_created_time    TIMESTAMP,
  ic_lesson_dw_created_time TIMESTAMP,
  ic_lesson_updated_time    TIMESTAMP,
  ic_lesson_dw_updated_time TIMESTAMP,
  ic_lesson_status          SMALLINT,
  ic_lesson_attach_status   SMALLINT,
  ic_lesson_type            SMALLINT,
  ic_uuid                   VARCHAR(36),
  lo_uuid                   VARCHAR(36)
);

create table alefdw.dim_ic_lesson_association
(
  ic_lesson_dw_id           BIGINT IDENTITY (1, 1),
  ic_lesson_created_time    TIMESTAMP,
  ic_lesson_dw_created_time TIMESTAMP,
  ic_lesson_updated_time    TIMESTAMP,
  ic_lesson_dw_updated_time TIMESTAMP,
  ic_lesson_status          SMALLINT,
  ic_lesson_attach_status   SMALLINT,
  ic_lesson_type            SMALLINT,
  ic_lesson_ic_dw_id        BIGINT,
  ic_lesson_lo_dw_id        BIGINT
)
diststyle all
compound sortkey (ic_lesson_dw_id);

create table alefdw_stage.rel_interim_checkpoint_rules
(
  rel_ic_rule_id          BIGINT IDENTITY (1, 1),
  ic_rule_created_time    TIMESTAMP,
  ic_rule_dw_created_time TIMESTAMP,
  ic_rule_updated_time    TIMESTAMP,
  ic_rule_dw_updated_time TIMESTAMP,
  ic_rule_status          SMALLINT,
  ic_rule_attach_status   SMALLINT,
  ic_rule_type            SMALLINT,
  ic_rule_resource_type   VARCHAR(50),
  ic_uuid                 VARCHAR(36),
  outcome_uuid            VARCHAR(36),
  ic_rule_no_questions    INT
);

create table alefdw.dim_interim_checkpoint_rules
(
  ic_rule_dw_id           BIGINT IDENTITY (1, 1),
  ic_rule_created_time    TIMESTAMP,
  ic_rule_dw_created_time TIMESTAMP,
  ic_rule_updated_time    TIMESTAMP,
  ic_rule_dw_updated_time TIMESTAMP,
  ic_rule_status          SMALLINT,
  ic_rule_attach_status   SMALLINT,
  ic_rule_type            SMALLINT,
  ic_rule_resource_type   VARCHAR(50),
  ic_rule_ic_dw_id        BIGINT,
  ic_rule_outcome_dw_id   BIGINT,
  ic_rule_no_questions    INT
)
  diststyle all
  compound sortkey (ic_rule_dw_id);

--- Score Breakdown ---
CREATE TABLE alefdw.fact_learning_score_breakdown
(
    fle_scbd_dw_id              BIGINT IDENTITY (1, 1),
    fle_scbd_created_time       TIMESTAMP,
    fle_scbd_dw_created_time    TIMESTAMP,
    fle_scbd_date_dw_id         BIGINT,
    fle_scbd_fle_dw_id          BIGINT,
    fle_scbd_fle_exp_id         VARCHAR(36),
    fle_scbd_fle_ls_id          VARCHAR(36),
    fle_scbd_question_dw_id     BIGINT,
    fle_scbd_code               VARCHAR(50),
    fle_scbd_time_spent         DOUBLE PRECISION,
    fle_scbd_hints_used         BOOLEAN,
    fle_scbd_max_score          DOUBLE PRECISION,
    fle_scbd_score              DOUBLE PRECISION,
    fle_scbd_lo_dw_id           BIGINT,
    fle_scbd_type               VARCHAR(250),
    fle_scbd_version            INT,
    fle_scbd_is_attended        BOOLEAN
)
    compound sortkey (fle_scbd_dw_id);

CREATE TABLE alefdw_stage.staging_learning_score_breakdown
(
    fle_scbd_dw_id              BIGINT IDENTITY (1, 1),
    fle_scbd_created_time       TIMESTAMP,
    fle_scbd_dw_created_time    TIMESTAMP,
    fle_scbd_date_dw_id         BIGINT,
    fle_scbd_fle_exp_id         VARCHAR(36),
    fle_scbd_fle_ls_id          VARCHAR(36),
    fle_scbd_question_id        VARCHAR(36),
    fle_scbd_code               VARCHAR(50),
    fle_scbd_time_spent         DOUBLE PRECISION,
    fle_scbd_hints_used         BOOLEAN,
    fle_scbd_max_score          DOUBLE PRECISION,
    fle_scbd_score              DOUBLE PRECISION,
    fle_scbd_lo_id              VARCHAR(36),
    fle_scbd_type               VARCHAR(250),
    fle_scbd_version            INT,
    fle_scbd_is_attended        BOOLEAN
)
    compound sortkey (fle_scbd_dw_id);

-- question pool
DROP TABLE IF EXISTS alefdw.dim_question_pool; -- Verify dim_question_pool is empty before running this

CREATE TABLE alefdw.dim_question_pool
(
    question_pool_dw_id                BIGINT IDENTITY (1, 1),
    question_pool_id                   VARCHAR(36),
    question_pool_name                 VARCHAR(256),
    question_pool_app_status           VARCHAR(24),
    question_pool_triggered_by         VARCHAR(24),
    question_pool_question_code_prefix VARCHAR(48),
    question_pool_status               INT,
    question_pool_created_time         TIMESTAMP,
    question_pool_dw_created_time      TIMESTAMP,
    question_pool_updated_time         TIMESTAMP,
    question_pool_deleted_time         TIMESTAMP,
    question_pool_dw_updated_time      TIMESTAMP
) compound sortkey (question_pool_dw_id);

create table if not exists alefdw_stage.rel_question(
    rel_question_id BIGINT IDENTITY(1, 1),
    question_created_time TIMESTAMP,
    question_updated_time TIMESTAMP,
    question_deleted_time TIMESTAMP,
    question_dw_created_time TIMESTAMP,
    question_dw_updated_time TIMESTAMP,
    question_status INTEGER,
    question_uuid VARCHAR(36),
    question_code VARCHAR(50),
    question_triggered_by VARCHAR(50),
    question_language VARCHAR(50),
    question_type VARCHAR(50),
    question_max_score DOUBLE PRECISION,
    question_version INTEGER,
    question_stage VARCHAR(50),
    question_variant VARCHAR(5),
    question_active_until TIMESTAMP,
    question_format_type VARCHAR(30),
    question_resource_type VARCHAR(10),
    question_summative_assessment BOOLEAN,
    question_difficulty_level VARCHAR(50),
    question_knowledge_dimensions VARCHAR(50),
    question_lexile_level VARCHAR(36),
    question_author VARCHAR(70),
    question_authored_date TIMESTAMP,
    question_skill_id VARCHAR(36),
    question_cefr_level VARCHAR(50),
    question_proficiency VARCHAR(50
                                )
    ) diststyle all
    sortkey(rel_question_id);

create table if not exists alefdw.dim_question(
    rel_question_dw_id BIGINT IDENTITY(1, 1),
    question_dw_id BIGINT,
    question_created_time TIMESTAMP,
    question_updated_time TIMESTAMP,
    question_deleted_time TIMESTAMP,
    question_dw_created_time TIMESTAMP,
    question_dw_updated_time TIMESTAMP,
    question_status INTEGER,
    question_id VARCHAR(36),
    question_code VARCHAR(50),
    question_triggered_by VARCHAR(50),
    question_language VARCHAR(50),
    question_type VARCHAR(50),
    question_max_score DOUBLE PRECISION,
    question_version INTEGER,
    question_stage VARCHAR(50),
    question_variant VARCHAR(5),
    question_active_until TIMESTAMP,
    question_format_type VARCHAR(30),
    question_resource_type VARCHAR(10),
    question_summative_assessment BOOLEAN,
    question_difficulty_level VARCHAR(50),
    question_knowledge_dimensions VARCHAR(50),
    question_lexile_level VARCHAR(36),
    question_author VARCHAR(70),
    question_authored_date TIMESTAMP,
    question_skill_id VARCHAR(36),
    question_cefr_level VARCHAR(50),
    question_proficiency VARCHAR(50)
    ) diststyle all
    sortkey(rel_question_dw_id);

ALTER TABLE alefdw_stage.rel_dw_id_mappings RENAME COLUMN class_id TO id;
ALTER TABLE alefdw_stage.rel_dw_id_mappings RENAME COLUMN class_dw_id TO dw_id;
ALTER TABLE alefdw_stage.rel_dw_id_mappings
    ADD COLUMN entity_type varchar(50);

ALTER TABLE alefdw.dim_step_instance drop step_instance_location;


ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_current_question_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_intest_progress DOUBLE PRECISION;
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_status INT;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_current_question_id VARCHAR(36);
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_intest_progress DOUBLE PRECISION;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_status INT;

ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_academic_year INT;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_academic_term INT;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_test_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_curriculum_subject_id BIGINT;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_curriculum_subject_name VARCHAR(255);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_status INT;

ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_academic_year INT;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_academic_term INT;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_test_id VARCHAR(36);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_curriculum_subject_id BIGINT;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_curriculum_subject_name VARCHAR(255);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_status INT;

--- To Be Executed Before Next Release ---
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_curriculum_subject_id INT;
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_curriculum_subject_name VARCHAR(36);

ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_curriculum_subject_id INT;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_curriculum_subject_name VARCHAR(36);

ALTER TABLE alefdw_stage.staging_adt_student_report DROP COLUMN fasr_scaler_version;
ALTER TABLE alefdw_stage.staging_adt_student_report RENAME COLUMN fasr_final_cefr TO fasr_final_result;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_final_uncertainty DOUBLE PRECISION;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_final_level VARCHAR(36);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_framework VARCHAR(36);

ALTER TABLE alefdw.fact_adt_student_report DROP COLUMN fasr_scaler_version;
ALTER TABLE alefdw.fact_adt_student_report RENAME COLUMN fasr_final_cefr TO fasr_final_result;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_final_uncertainty DOUBLE PRECISION;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_final_level VARCHAR(36);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_framework VARCHAR(36);

alter table alefdw_stage.rel_class_user add column class_user_attach_status INT;
alter table alefdw.dim_class_user add column class_user_attach_status INT;

alter table alefdw.dim_skill_association add column skill_association_is_previous BOOLEAN;
alter table alefdw.dim_interim_checkpoint drop column ic_ip_id;


alter table alefdw.dim_question_pool alter column question_pool_triggered_by type VARCHAR(36);


alter table alefdw.fact_experience_submitted add column fes_ls_id VARCHAR(36);
alter table alefdw.fact_experience_submitted add column exp_uuid VARCHAR(36);

alter table alefdw.fact_lesson_feedback add column lesson_feedback_fle_ls_uuid VARCHAR(36);

alter table alefdw.dim_role add column role_name VARCHAR(50);
update alefdw.dim_role set role_name = role_id;

alter table alefdw.fact_adt_student_report add column fasr_fle_ls_uuid VARCHAR(36);

alter table alefdw.fact_adt_next_question add column fanq_fle_ls_uuid VARCHAR(36);

--- To Be Executed Before Next Release ---

alter table alefdw.fact_star_awarded add column fsa_stars int;
alter table alefdw_stage.staging_star_awarded add column fsa_stars int;

alter table alefdw.dim_role drop column role_id;
-----

create table alefdw.dim_weekly_goal_type
(
    weekly_goal_type_dw_id bigint identity(1,1),
    weekly_goal_type_created_time timestamp,
    weekly_goal_type_dw_created_time timestamp,
    weekly_goal_type_id varchar(36),
    weekly_goal_type_tenant_id varchar(36),
    weekly_goal_type_total_activity_count int,
    weekly_goal_type_stars_to_award int
);

create table alefdw_stage.staging_weekly_goal
(
    fwg_dw_id bigint identity(1,1),
    fwg_created_time timestamp,
    fwg_dw_created_time timestamp,
    fwg_id varchar(36),
    fwg_action_status int,
    fwg_type_id varchar(36),
    fwg_student_id varchar(36),
    fwg_tenant_id varchar(36),
    fwg_class_id varchar(36),
    fwg_star_earned int
);

create table alefdw.fact_weekly_goal
(
    fwg_dw_id bigint identity(1,1),
    fwg_created_time timestamp,
    fwg_dw_created_time timestamp,
    fwg_id varchar(36),
    fwg_action_status int,
    fwg_type_dw_id bigint,
    fwg_student_dw_id bigint,
    fwg_tenant_dw_id bigint,
    fwg_class_dw_id bigint,
    fwg_star_earned int
);

create table alefdw.fact_weekly_goal_activity
(
    fwga_dw_id bigint identity(1,1),
    fwga_created_time timestamp,
    fwga_dw_created_time timestamp,
    fwga_weekly_goal_id varchar(36),
    fwga_completed_activity_id varchar(36),
    fwga_weekly_goal_status int
);

ALTER TABLE alefdw_stage.rel_class  ADD COLUMN class_material_id VARCHAR(36);
ALTER TABLE alefdw_stage.rel_class  ADD COLUMN class_material_type VARCHAR(20);
ALTER TABLE alefdw.dim_class  ADD COLUMN class_material_id VARCHAR(36);
ALTER TABLE alefdw.dim_class  ADD COLUMN class_material_type VARCHAR(20);

ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_material_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_material_id VARCHAR(36);
ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_material_type VARCHAR(20);

ALTER TABLE alefdw_stage.staging_experience_submitted ADD COLUMN fes_material_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_experience_submitted ADD COLUMN fes_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_experience_submitted ADD COLUMN fes_material_id VARCHAR(36);
ALTER TABLE alefdw.fact_experience_submitted ADD COLUMN fes_material_type VARCHAR(20);

ALTER TABLE alefdw.dim_interim_checkpoint ADD COLUMN ic_material_type VARCHAR(20);

CREATE TABLE alefdw.dim_pathway (
    pathway_dw_id BIGINT IDENTITY (1,1),
    pathway_created_time TIMESTAMP,
    pathway_dw_created_time TIMESTAMP,
    pathway_updated_time TIMESTAMP,
    pathway_dw_updated_time TIMESTAMP,
    pathway_name VARCHAR(255),
    pathway_subject_id BIGINT,
    pathway_code VARCHAR(50),
    pathway_goal VARCHAR(50),
    pathway_status VARCHAR(50),
    pathway_type VARCHAR(50)
) diststyle all sortkey (pathway_dw_id);

ALTER TABLE alefdw_stage.staging_student_activities ADD COLUMN fsta_is_completion_node BOOLEAN;
ALTER TABLE alefdw_stage.staging_student_activities ADD COLUMN fsta_is_flexible_lesson BOOLEAN;
ALTER TABLE alefdw_stage.fact_student_activities ADD COLUMN fsta_academic_calendar_id varchar(36);
ALTER TABLE alefdw_stage.fact_student_activities ADD COLUMN fsta_teaching_period_id varchar(36);
ALTER TABLE alefdw_stage.fact_student_activities ADD COLUMN fsta_teaching_period_title varchar(50);

ALTER TABLE alefdw.fact_student_activities ADD COLUMN fsta_is_completion_node BOOLEAN;
ALTER TABLE alefdw.fact_student_activities ADD COLUMN fsta_is_flexible_lesson BOOLEAN;
ALTER TABLE alefdw.fact_student_activities ADD COLUMN fsta_academic_calendar_id varchar(36);
ALTER TABLE alefdw.fact_student_activities ADD COLUMN fsta_teaching_period_id varchar(36);
ALTER TABLE alefdw.fact_student_activities ADD COLUMN fsta_teaching_period_title varchar(50);






ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_final_standard_error DOUBLE PRECISION;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_language VARCHAR(50);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN school_uuid VARCHAR(36);

ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_final_standard_error DOUBLE PRECISION;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_language VARCHAR(50);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_school_dw_id BIGINT;

ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_language VARCHAR(50);
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_standard_error DOUBLE PRECISION;

ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_language VARCHAR(50);
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_standard_error DOUBLE PRECISION;

CREATE TABLE alefdw.dim_organization
(
    organization_dw_id            BIGINT IDENTITY(1,1),
    organization_created_time     TIMESTAMP,
    organization_deleted_time     TIMESTAMP,
    organization_updated_time     TIMESTAMP,
    organization_dw_created_time  TIMESTAMP,
    organization_dw_updated_time  TIMESTAMP,
    organization_dw_deleted_time  TIMESTAMP,
    organization_id               VARCHAR(36),
    organization_name             VARCHAR(50),
    organization_code            VARCHAR(50),
    organization_email            VARCHAR(50),
    organization_country          VARCHAR(20),
    organization_tenant_code      VARCHAR(20),
    organization_status           INT,
    organization_created_by       VARCHAR(36),
    organization_updated_by       VARCHAR(36)
) diststyle all
compound sortkey(organization_dw_id);

create table alefdw.dim_content_repository (
                                               content_repository_dw_id BIGINT IDENTITY(1,1),
                                               content_repository_id varchar(36),
                                               content_repository_name varchar(50),
                                               content_repository_organization varchar(50),
                                               content_repository_created_time TIMESTAMP,
                                               content_repository_dw_created_time TIMESTAMP,
                                               content_repository_updated_time TIMESTAMP,
                                               content_repository_dw_updated_time TIMESTAMP,
                                               content_repository_deleted_time TIMESTAMP,
                                               content_repository_dw_deleted_time TIMESTAMP
)diststyle all
compound sortkey(content_repository_dw_id);

ALTER TABLE alefdw.dim_school ADD COLUMN school_content_repository_dw_id BIGINT;
ALTER TABLE alefdw.dim_school ADD COLUMN school_organization_dw_id BIGINT;


CREATE TABLE alefdw_stage.staging_level_completed (
    flc_dw_id BIGINT IDENTITY (1,1),
    flc_created_time TIMESTAMP,
    flc_dw_created_time TIMESTAMP,
    flc_date_dw_id BIGINT,
    flc_completed_on TIMESTAMP,
    flc_tenant_id VARCHAR(36),
    flc_student_id VARCHAR(36),
    flc_class_id VARCHAR(36),
    flc_pathway_id VARCHAR(36),
    flc_level_id VARCHAR(36),
    flc_total_stars INT
) SORTKEY (flc_dw_id);

CREATE TABLE alefdw.fact_level_completed (
    flc_dw_id BIGINT IDENTITY (1,1),
    flc_created_time TIMESTAMP,
    flc_dw_created_time TIMESTAMP,
    flc_date_dw_id BIGINT,
    flc_completed_on TIMESTAMP,
    flc_tenant_dw_id BIGINT,
    flc_student_dw_id BIGINT,
    flc_class_dw_id BIGINT,
    flc_pathway_dw_id BIGINT,
    flc_level_dw_id BIGINT,
    flc_total_stars INT
) SORTKEY (flc_dw_id);

CREATE TABLE alefdw_stage.staging_pathway_activity_completed (
    fpac_dw_id BIGINT IDENTITY (1, 1),
    fpac_created_time TIMESTAMP,
    fpac_dw_created_time TIMESTAMP,
    fpac_date_dw_id BIGINT,
    fpac_tenant_id VARCHAR(36),
    fpac_student_id VARCHAR(36),
    fpac_class_id VARCHAR(36),
    fpac_pathway_id VARCHAR(36),
    fpac_level_id VARCHAR(36),
    fpac_activity_id VARCHAR(36),
    fpac_activity_type INT,
    fpac_score DOUBLE PRECISION
) SORTKEY (fpac_dw_id);

CREATE TABLE alefdw.fact_pathway_activity_completed (
    fpac_dw_id BIGINT IDENTITY (1, 1),
    fpac_created_time TIMESTAMP,
    fpac_dw_created_time TIMESTAMP,
    fpac_date_dw_id BIGINT,
    fpac_tenant_dw_id BIGINT,
    fpac_student_dw_id BIGINT,
    fpac_class_dw_id BIGINT,
    fpac_pathway_dw_id BIGINT,
    fpac_level_dw_id BIGINT,
    fpac_activity_dw_id BIGINT,
    fpac_activity_type INT,
    fpac_score DOUBLE PRECISION
) SORTKEY (fpac_dw_id);

CREATE TABLE alefdw_stage.staging_levels_recommended (
    flr_dw_id BIGINT IDENTITY (1, 1),
    flr_created_time TIMESTAMP,
    flr_dw_created_time TIMESTAMP,
    flr_date_dw_id BIGINT,
    flr_recommended_on TIMESTAMP,
    flr_tenant_id VARCHAR(36),
    flr_student_id VARCHAR(36),
    flr_class_id VARCHAR(36),
    flr_pathway_id VARCHAR(36),
    flr_completed_level_id VARCHAR(36),
    flr_level_id VARCHAR(36),
    flr_status INT,
    flr_recommendation_type INT
) SORTKEY (flr_dw_id);

CREATE TABLE alefdw.fact_levels_recommended (
    flr_dw_id BIGINT IDENTITY (1, 1),
    flr_created_time TIMESTAMP,
    flr_dw_created_time TIMESTAMP,
    flr_date_dw_id BIGINT,
    flr_recommended_on TIMESTAMP,
    flr_tenant_dw_id BIGINT,
    flr_student_dw_id BIGINT,
    flr_class_dw_id BIGINT,
    flr_pathway_dw_id BIGINT,
    flr_completed_level_dw_id BIGINT,
    flr_level_dw_id BIGINT,
    flr_status INT,
    flr_recommendation_type INT
) SORTKEY (flr_dw_id);


CREATE TABLE alefdw_stage.rel_pathway
(
    rel_pathway_dw_id          BIGINT IDENTITY (1, 1),
    pathway_id                 varchar(36),
    pathway_creation_stage     INT,
    pathway_status             INT,
    pathway_name               varchar(50),
    pathway_code               varchar(50),
    pathway_subject_id         Int,
    pathway_organization       varchar(50),
    pathway_content_repository varchar(50),
    pathway_created_time       TIMESTAMP,
    pathway_deleted_time       TIMESTAMP,
    pathway_updated_time       TIMESTAMP,
    pathway_dw_created_time    TIMESTAMP,
    pathway_dw_updated_time    TIMESTAMP,
    pathway_dw_deleted_time    TIMESTAMP
);

CREATE TABLE alefdw.dim_pathway
(
    rel_pathway_dw_id                BIGINT IDENTITY (1, 1),
    pathway_dw_id                    BIGINT,
    pathway_id                       varchar(36),
    pathway_creation_stage           INT,
    pathway_status                   INT,
    pathway_name                     varchar(50),
    pathway_code                     varchar(50),
    pathway_subject_id               Int,
    pathway_organization_dw_id       BIGINT,
    pathway_content_repository_dw_id BIGINT,
    pathway_created_time             TIMESTAMP,
    pathway_deleted_time             TIMESTAMP,
    pathway_updated_time             TIMESTAMP,
    pathway_dw_created_time          TIMESTAMP,
    pathway_dw_updated_time          TIMESTAMP,
    pathway_dw_deleted_time          TIMESTAMP
);

CREATE TABLE alefdw_stage.rel_pathway_level
(
    rel_pathway_level_dw_id       BIGINT IDENTITY (1, 1),
    pathway_level_id              varchar(36),
    pathway_level_pathway_id      varchar(36),
    pathway_level_index           int,
    pathway_level_title           varchar(50),
    pathway_level_pacing          varchar(50),
    pathway_level_domain          varchar(50),
    pathway_level_sequence        INT,
    pathway_level_status          INT,
    pathway_level_created_time    TIMESTAMP,
    pathway_level_deleted_time    TIMESTAMP,
    pathway_level_updated_time    TIMESTAMP,
    pathway_level_dw_created_time TIMESTAMP,
    pathway_level_dw_updated_time TIMESTAMP,
    pathway_level_dw_deleted_time TIMESTAMP
);

CREATE TABLE alefdw.dim_pathway_level
(
    rel_pathway_level_dw_id       BIGINT IDENTITY (1, 1),
    pathway_level_dw_id           BIGINT,
    pathway_level_id              varchar(36),
    pathway_level_pathway_id      varchar(36),
    pathway_level_index           int,
    pathway_level_title           varchar(50),
    pathway_level_pacing          varchar(50),
    pathway_level_domain          varchar(50),
    pathway_level_sequence        INT,
    pathway_level_status          INT,
    pathway_level_created_time    TIMESTAMP,
    pathway_level_deleted_time    TIMESTAMP,
    pathway_level_updated_time    TIMESTAMP,
    pathway_level_dw_created_time TIMESTAMP,
    pathway_level_dw_updated_time TIMESTAMP,
    pathway_level_dw_deleted_time TIMESTAMP
);

CREATE TABLE alefdw.dim_pathway_module_association
(
    pma_dw_id           BIGINT IDENTITY (1, 1),
    pma_pathway_id      varchar(36),
    pma_module_uuid     varchar(36),
    pma_module_id       INT,
    pma_module_type     INT,
    pma_created_time    TIMESTAMP,
    pma_dw_created_time TIMESTAMP,
    pma_status          INT,
)

CREATE TABLE alefdw.dim_pathway_level_activity_association
(
    plaa_dw_id           BIGINT IDENTITY(1,1),
    plaa_level_id        varchar(36),
    plaa_activity_id     varchar(36),
    plaa_activity_type   Int,
    plaa_activity_pacing varchar(50),
    plaa_created_time    TIMESTAMP,
    plaa_dw_created_time TIMESTAMP,
    plaa_status          INT,
)

ALTER TABLE alefdw_stage.rel_instructional_plan ADD COLUMN instructional_plan_content_repository_id varchar(36);
ALTER TABLE alefdw.dim_instructional_plan ADD COLUMN instructional_plan_content_repository_id varchar(36);
ALTER TABLE alefdw.dim_term ADD COLUMN term_content_repository_dw_id bigint;
ALTER TABLE alefdw.dim_term ADD COLUMN term_content_repository_id varchar(36);
ALTER TABLE alefdw.dim_class_category ADD COLUMN class_category_organization_code varchar(20);

create table alefdw_stage.rel_content_repository (
                                                         rel_content_repository_id BIGINT IDENTITY(1,1),
                                                         content_repository_uuid varchar(36),
                                                         content_repository_name varchar(50),
                                                         content_repository_organization varchar(50),
                                                         content_repository_created_time TIMESTAMP,
                                                         content_repository_dw_created_time TIMESTAMP,
                                                         content_repository_updated_time TIMESTAMP,
                                                         content_repository_dw_updated_time TIMESTAMP,
                                                         content_repository_status int
)diststyle all
compound sortkey(rel_content_repository_id);

ALTER TABLE alefdw.dim_content_repository RENAME TO alefdw.dim_old_content_repository;

create table alefdw.dim_content_repository (
                                               rel_content_repository_dw_id BIGINT IDENTITY(1,1),
                                               content_repository_dw_id BIGINT,
                                               content_repository_name varchar(50),
                                               content_repository_status int,
                                               content_repository_organization varchar(50),
                                               content_repository_created_time TIMESTAMP,
                                               content_repository_dw_created_time TIMESTAMP,
                                               content_repository_updated_time TIMESTAMP,
                                               content_repository_dw_updated_time TIMESTAMP
)diststyle all
compound sortkey(rel_content_repository_dw_id);

insert into alefdw.dim_content_repository (select content_repository_dw_id,
       content_repository_name,
       content_repository_organization,
       content_repository_created_time,
       content_repository_dw_created_time,
       content_repository_updated_time,
       content_repository_dw_updated_time
from alefdw.dim_old_content_repository);

drop table alefdw.dim_old_content_repository;

alter table alefdw_stage.staging_learning_score_breakdown alter column fle_scbd_code type varchar(128);
alter table alefdw_stage.rel_question alter column question_code type varchar(128);
alter table alefdw.dim_question alter column question_code type varchar(128);
alter table alefdw.dim_question_pool alter column question_pool_question_code_prefix type varchar(128);
alter table alefdw.fact_learning_score_breakdown alter column fle_scbd_code type varchar(128);

alter table alefdw_stage.staging_pathway_activity_completed add column fpac_time_spent integer;
alter table alefdw.fact_pathway_activity_completed add column fpac_time_spent integer;


ALTER TABLE alefdw_stage.rel_user ADD COLUMN user_type VARCHAR(50);
ALTER TABLE alefdw_stage.rel_user ADD COLUMN user_created_time TIMESTAMP;

CREATE TABLE alefdw_stage.staging_announcement
(
    fa_dw_id          BIGINT IDENTITY (1, 1),
    fa_created_time       TIMESTAMP,
    fa_dw_created_time    TIMESTAMP,
    fa_status             INT,
    fa_tenant_id          varchar(36),
    fa_id                 varchar(36),
    fa_admin_id           varchar(36),
    fa_role_id            varchar(36),
    fa_recipient_type     INT,
    fa_recipient_type_description varchar(50),
    fa_recipient_id       varchar(36),
    fa_has_attachment     boolean
);

CREATE TABLE alefdw.fact_announcement
(
    fa_dw_id          BIGINT IDENTITY (1, 1),
    fa_created_time       TIMESTAMP,
    fa_dw_created_time    TIMESTAMP,
    fa_status             INT,
    fa_tenant_dw_id       INT,
    fa_id                 varchar(36),
    fa_admin_dw_id        INT,
    fa_role_dw_id         INT,
    fa_recipient_type     INT,
    fa_recipient_type_description varchar(50),
    fa_recipient_dw_id    INT,
    fa_has_attachment     boolean
);

ALTER TABLE alefdw_stage.rel_pathway ADD COLUMN pathway_lang_code VARCHAR(10);
ALTER TABLE alefdw.dim_pathway ADD COLUMN pathway_lang_code VARCHAR(10);

ALTER TABLE alefdw.dim_pathway_module_association RENAME COLUMN pma_module_uuid TO pma_module_activity_uuid;
ALTER TABLE alefdw.dim_pathway_module_association RENAME COLUMN pma_module_id TO pma_module_activity_id;

ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_module_id VARCHAR(36);
ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_max_attempts INT;
ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_module_pacing VARCHAR(16);
ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_module_index INT;
ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_pathway_version VARCHAR(10);
ALTER TABLE alefdw.dim_pathway_module_association ADD COLUMN pma_attach_status INT;

ALTER TABLE alefdw_stage.rel_pathway ALTER COLUMN pathway_name TYPE VARCHAR(255);
ALTER TABLE alefdw.dim_pathway ALTER COLUMN pathway_name TYPE VARCHAR(255);

-- Redshift:
DELETE FROM alefdw.dim_pathway WHERE pathway_creation_stage in (1,2);
ALTER TABLE alefdw.dim_pathway DROP COLUMN pathway_creation_stage;


ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_attach_status INT;
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_pathway_version VARCHAR(10);
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_activity_index INT;
ALTER TABLE alefdw.dim_pathway_level_activity_association DROP COLUMN plaa_deleted_time;



ALTER TABLE alefdw_stage.staging_announcement ADD COLUMN fa_type INT;
ALTER TABLE alefdw.fact_announcement ADD COLUMN fa_type INT;


ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_state SMALLINT;
ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_state SMALLINT;

ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_is_parent_deleted boolean;

ALTER TABLE alefdw_stage.rel_pathway_level ADD COLUMN pathway_level_attach_status INT DEFAULT 1;
ALTER TABLE alefdw_stage.rel_pathway_level ADD COLUMN pathway_level_pathway_version VARCHAR(10);
ALTER TABLE alefdw.dim_pathway_level ADD COLUMN pathway_level_attach_status INT DEFAULT 1;
ALTER TABLE alefdw.dim_pathway_level ADD COLUMN pathway_level_pathway_version VARCHAR(10);

ALTER TABLE alefdw.fact_pathway_activity_completed ADD COLUMN fpac_learning_session_id VARCHAR(36);
ALTER TABLE alefdw.fact_pathway_activity_completed ADD COLUMN fpac_attempt INT;

ALTER TABLE alefdw_stage.staging_pathway_activity_completed ADD COLUMN fpac_learning_session_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_pathway_activity_completed ADD COLUMN fpac_attempt INT;

ALTER TABLE alefdw_stage.rel_pathway_level ADD COLUMN pathway_level_grade VARCHAR(10);
ALTER TABLE alefdw.dim_pathway_level ADD COLUMN pathway_level_grade VARCHAR(10);


ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_pathway_id VARCHAR(36);
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_pathway_dw_id BIGINT;

CREATE TABLE alefdw_stage.rel_pathway_level_activity_association
(
    rel_plaa_dw_id       BIGINT IDENTITY(1,1),
    plaa_pathway_id      VARCHAR(36),
    plaa_level_id        VARCHAR(36),
    plaa_activity_id     VARCHAR(36),
    plaa_activity_type   INT,
    plaa_activity_pacing VARCHAR(50),
    plaa_created_time    TIMESTAMP,
    plaa_dw_created_time TIMESTAMP,
    plaa_updated_time    TIMESTAMP,
    plaa_dw_updated_time    TIMESTAMP,
    plaa_status          INT,
    plaa_attach_status   INT,
    plaa_pathway_version VARCHAR(10),
    plaa_activity_index INT,
    plaa_is_parent_deleted BOOLEAN
);

ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_level_dw_id BIGINT;
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_activity_dw_id BIGINT;

ALTER TABLE alefdw_stage.rel_pathway DROP COLUMN pathway_creation_stage;

ALTER TABLE alefdw.dim_content_repository RENAME TO dim_old_content_repository;

create table alefdw.dim_content_repository
(
rel_content_repository_dw_id       BIGINT IDENTITY (1,1),
content_repository_created_time    TIMESTAMP,
content_repository_dw_created_time TIMESTAMP,
content_repository_updated_time    TIMESTAMP,
content_repository_dw_updated_time TIMESTAMP,
content_repository_dw_id           BIGINT,
content_repository_id              VARCHAR(36),
content_repository_status          int,
content_repository_name            varchar(50),
content_repository_organization    varchar(50)
) diststyle all
compound sortkey (rel_content_repository_dw_id);

insert into alefdw.dim_content_repository (content_repository_created_time, content_repository_dw_created_time, content_repository_updated_time, content_repository_dw_updated_time, content_repository_dw_id, content_repository_status, content_repository_name, content_repository_organization)
select content_repository_created_time,
       content_repository_dw_created_time,
       content_repository_updated_time,
       content_repository_dw_updated_time,
       content_repository_dw_id,
       content_repository_status,
       content_repository_name,
       content_repository_organization from alefdw.dim_old_content_repository;

drop table alefdw.dim_old_content_repository;

update alefdw.dim_content_repository
    set content_repository_id = ids.id
    from alefdw.dim_content_repository cr
    join alefdw_stage.rel_dw_id_mappings ids
      on cr.content_repository_dw_id = ids.dw_id and
      ids.entity_type = 'content-repository';

ALTER TABLE alefdw_stage.rel_admin ADD COLUMN admin_exclude_from_report BOOLEAN DEFAULT FALSE;
ALTER TABLE alefdw.dim_admin ADD COLUMN admin_exclude_from_report BOOLEAN DEFAULT FALSE;

ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_attempt int;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_attempt int;

ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_attempt int;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_final_grade int;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_attempt int;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_final_grade int;


CREATE TABLE alefdw.dim_badge
(
    bdg_dw_id BIGINT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY(1, 1)
    ,bdg_id VARCHAR(36)
    ,bdg_tier VARCHAR(36)
    ,bdg_grade VARCHAR(36)
    ,bdg_type VARCHAR(36)
    ,bdg_tenant_dw_id BIGINT
    ,bdg_title VARCHAR(36)
    ,bdg_category VARCHAR(36)
    ,bdg_threshold INTEGER
    ,bdg_status INTEGER
    ,bdg_created_time TIMESTAMP ENCODE AZ64
    ,bdg_deleted_time TIMESTAMP ENCODE AZ64
    ,bdg_dw_created_time TIMESTAMP ENCODE AZ64 DEFAULT CURRENT_TIMESTAMP
    ,bdg_active_until TIMESTAMP ENCODE AZ64 DEFAULT CURRENT_TIMESTAMP
)
DISTSTYLE KEY DISTKEY (bdg_dw_id)
SORTKEY (bdg_dw_id);



CREATE TABLE alefdw_stage.staging_badge_awarded (
    fba_dw_id BIGINT IDENTITY(1, 1),
    fba_id VARCHAR(36),
    fba_created_time TIMESTAMP,
    fba_dw_created_time TIMESTAMP,
    fba_date_dw_id BIGINT,
    fba_student_id VARCHAR(36),
    fba_tenant_id VARCHAR(36),
    fba_badge_type VARCHAR(100),
    fba_badge_type_id VARCHAR(36),
    fba_tier VARCHAR(50),
    fba_academic_year_id VARCHAR(36),
    fba_section_id VARCHAR(36),
    fba_grade_id VARCHAR(36),
    fba_school_id VARCHAR(36)
) SORTKEY (fba_dw_id);

CREATE TABLE alefdw.fact_badge_awarded
(
    fba_dw_id BIGINT IDENTITY(1, 1),
    fba_id VARCHAR(36),
    fba_created_time TIMESTAMP ENCODE AZ64,
    fba_badge_dw_id BIGINT,
    fba_student_dw_id BIGINT,
    fba_school_dw_id BIGINT,
    fba_grade_dw_id BIGINT,
    fba_section_dw_id BIGINT,
    fba_tenant_dw_id BIGINT,
    fba_academic_year_dw_id  BIGINT,
    fba_content_repository_dw_id BIGINT,
    fba_organization_dw_id BIGINT,
    fba_date_dw_id BIGINT,
    fba_dw_created_time TIMESTAMP ENCODE AZ64 DEFAULT CURRENT_TIMESTAMP
)
    DISTSTYLE KEY DISTKEY (fba_dw_id)
    SORTKEY (fba_created_time);

ALTER TABLE alefdw_stage.rel_instructional_plan ADD COLUMN content_repository_uuid VARCHAR(36);
ALTER TABLE alefdw.dim_instructional_plan ADD COLUMN instructional_plan_content_repository_dw_id BIGINT;

create table alefdw.fact_tutor_user_context
(   ftc_dw_id BIGINT IDENTITY(1, 1),
    ftc_created_time TIMESTAMP,
    ftc_dw_created_time TIMESTAMP,
    ftc_tenant_dw_id BIGINT,
    ftc_user_dw_id BIGINT,
    ftc_role VARCHAR(20),
    ftc_context_id VARCHAR(36),
    ftc_school_dw_id  BIGINT,
    ftc_grade_dw_id BIGINT,
    ftc_grade BIGINT,
    ftc_subject_dw_id BIGINT,
    ftc_subject BIGINT,
    ftc_language VARCHAR(20)
)
DISTSTYLE KEY DISTKEY (ftc_dw_id)
SORTKEY (ftc_created_time);

create table alefdw_stage.staging_tutor_user_context
(   ftc_dw_id BIGINT IDENTITY(1, 1),
    ftc_created_time TIMESTAMP,
    ftc_dw_created_time TIMESTAMP,
    ftc_tenant_id VARCHAR(36),
    ftc_user_id VARCHAR(36),
    ftc_role VARCHAR(20),
    ftc_context_id VARCHAR(36),
    ftc_school_id VARCHAR(36),
    ftc_grade_id VARCHAR(36),
    ftc_grade BIGINT,
    ftc_subject_id VARCHAR(36),
    ftc_subject BIGINT,
    ftc_language VARCHAR(20)
)
SORTKEY (ftc_dw_id);

create table alefdw.fact_tutor_session (
    fts_dw_id BIGINT IDENTITY(1, 1),
    fts_created_time TIMESTAMP,
    fts_dw_created_time TIMESTAMP,
    fts_date_dw_id BIGINT,
    fts_session_id VARCHAR(36),
    fts_tenant_dw_id BIGINT,
    fts_school_dw_id BIGINT,
    fts_user_dw_id BIGINT,
    fts_grade_dw_id BIGINT,
    fts_context_id VARCHAR(36),
    fts_role VARCHAR(20),
    fts_grade INT,
    fts_subject_dw_id BIGINT,
    fts_subject VARCHAR(20),
    fts_language VARCHAR(20),
    fts_session_state VARCHAR(20),
    fts_activity_dw_id BIGINT,
    fts_activity_status VARCHAR(20),
    fts_material_id VARCHAR(36),
    fts_material_type VARCHAR(20),
    fts_level_dw_id BIGINT,
    fts_outcome_dw_id BIGINT
)
DISTSTYLE KEY DISTKEY (fts_dw_id)
SORTKEY (fts_created_time);

create table alefdw_stage.staging_tutor_session (
    fts_dw_id BIGINT IDENTITY(1, 1),
    fts_created_time TIMESTAMP,
    fts_dw_created_time TIMESTAMP,
    fts_date_dw_id BIGINT,
    fts_session_id VARCHAR(36),
    fts_tenant_id VARCHAR(36),
    fts_school_id VARCHAR(36),
    fts_user_id VARCHAR(36),
    fts_grade_id VARCHAR(36),
    fts_context_id VARCHAR(36),
    fts_role VARCHAR(20),
    fts_grade INT,
    fts_subject_id VARCHAR(36),
    fts_subject VARCHAR(20),
    fts_language VARCHAR(20),
    fts_session_state VARCHAR(20),
    fts_activity_id VARCHAR(36),
    fts_activity_status VARCHAR(20),
    fts_material_id VARCHAR(36),
    fts_material_type VARCHAR(20),
    fts_level_id VARCHAR(36),
    fts_outcome_id VARCHAR(36)
)
SORTKEY (fts_dw_id);

create table alefdw.fact_tutor_conversation(ftc_dw_id BIGINT IDENTITY(1, 1),
    ftc_created_time TIMESTAMP,
    ftc_dw_created_time TIMESTAMP,
    ftc_date_dw_id BIGINT,
    ftc_tenant_dw_id BIGINT,
    ftc_school_dw_id BIGINT,
    ftc_user_dw_id BIGINT,
    ftc_role VARCHAR(20),
    ftc_grade INT,
    ftc_grade_dw_id BIGINT,
    ftc_context_id VARCHAR(36),
    ftc_session_id VARCHAR(36),
    ftc_subject_dw_id BIGINT,
    ftc_subject VARCHAR(20),
    ftc_language VARCHAR(20),
    ftc_activity_dw_id BIGINT,
    ftc_activity_status VARCHAR(20),
    ftc_material_id VARCHAR(36),
    ftc_material_type VARCHAR(20),
    ftc_level_dw_id BIGINT,
    ftc_outcome_dw_id BIGINT,
    ftc_conversation_max_tokens INT,
    ftc_conversation_token_count INT,
    ftc_system_prompt_tokens INT,
    ftc_message_language VARCHAR(30),
    ftc_message_feedback BOOLEAN,
    ftc_user_message_source VARCHAR(30),
    ftc_user_message_tokens INT,
    ftc_user_message_timestamp TIMESTAMP,
    ftc_bot_message_source VARCHAR(30),
    ftc_bot_message_tokens INT,
    ftc_bot_message_timestamp TIMESTAMP,
    ftc_bot_message_confidence DOUBLE PRECISION,
    ftc_bot_message_response_time DOUBLE PRECISION,
    ftc_bot_suggestions_confidence DOUBLE PRECISION,
    ftc_bot_suggestions_response_time DOUBLE PRECISION
)
DISTSTYLE KEY DISTKEY (ftc_dw_id)
SORTKEY (ftc_created_time);

create table alefdw_stage.staging_tutor_conversation(
    ftc_dw_id BIGINT IDENTITY(1, 1),
    ftc_created_time TIMESTAMP,
    ftc_dw_created_time TIMESTAMP,
    ftc_date_dw_id BIGINT,
    ftc_tenant_id VARCHAR(36),
    ftc_school_id VARCHAR(36),
    ftc_user_id VARCHAR(36),
    ftc_role VARCHAR(20),
    ftc_grade INT,
    ftc_grade_id VARCHAR(36),
    ftc_context_id VARCHAR(36),
    ftc_session_id VARCHAR(36),
    ftc_subject_id bigint,
    ftc_subject VARCHAR(20),
    ftc_language VARCHAR(20),
    ftc_activity_id VARCHAR(36),
    ftc_activity_status VARCHAR(20),
    ftc_material_id VARCHAR(36),
    ftc_material_type VARCHAR(20),
    ftc_level_id VARCHAR(36),
    ftc_outcome_id VARCHAR(36),
    ftc_conversation_max_tokens INT,
    ftc_conversation_token_count INT,
    ftc_system_prompt_tokens INT,
    ftc_message_language VARCHAR(30),
    ftc_message_feedback BOOLEAN,
    ftc_user_message_source VARCHAR(30),
    ftc_user_message_tokens INT,
    ftc_user_message_timestamp TIMESTAMP,
    ftc_bot_message_source VARCHAR(30),
    ftc_bot_message_tokens INT,
    ftc_bot_message_timestamp TIMESTAMP,
    ftc_bot_message_confidence DOUBLE PRECISION,
    ftc_bot_message_response_time DOUBLE PRECISION,
    ftc_bot_suggestions_confidence DOUBLE PRECISION,
    ftc_bot_suggestions_response_time DOUBLE PRECISION
)
SORTKEY (ftc_dw_id);

CREATE TABLE alefdw.fact_pathway_leaderboard(
    fpl_dw_id BIGINT IDENTITY(1, 1),
    fpl_created_time TIMESTAMP,
    fpl_dw_created_time TIMESTAMP,
    fpl_date_dw_id BIGINT,
    fpl_id VARCHAR(36),
    fpl_student_dw_id BIGINT,
    fpl_pathway_dw_id BIGINT,
    fpl_class_dw_id BIGINT,
    fpl_grade_dw_id BIGINT,
    fpl_academic_year_dw_id BIGINT,
    fpl_start_date DATE,
    fpl_end_date DATE,
    fpl_order SMALLINT,
    fpl_level_competed_count SMALLINT,
    fpl_average_score DOUBLE PRECISION,
    fpl_total_stars SMALLINT,
    fpl_tenant_dw_id BIGINT
)
DISTSTYLE KEY DISTKEY (fpl_dw_id)
SORTKEY (fpl_created_time);

CREATE TABLE alefdw_stage.staging_pathway_leaderboard(
    fpl_staging_id BIGINT IDENTITY(1, 1),
    fpl_created_time TIMESTAMP,
    fpl_dw_created_time TIMESTAMP,
    fpl_date_dw_id BIGINT,
    fpl_id VARCHAR(36),
    fpl_student_id VARCHAR(36),
    fpl_pathway_id VARCHAR(36),
    fpl_class_id VARCHAR(36),
    fpl_grade_id VARCHAR(36),
    fpl_academic_year_id VARCHAR(36),
    fpl_start_date DATE,
    fpl_end_date DATE,
    fpl_order SMALLINT,
    fpl_level_competed_count SMALLINT,
    fpl_average_score DOUBLE PRECISION,
    fpl_total_stars SMALLINT,
    fpl_tenant_id VARCHAR(36)
)
SORTKEY (fpl_staging_id);

CREATE TABLE alefdw_stage.rel_pathway_curriculum
(
    rel_pc_dw_id          BIGINT IDENTITY (1, 1),
    pc_pathway_id         VARCHAR(36),
    pc_curr_id            BIGINT,
    pc_curr_grade_id      BIGINT,
    pc_curr_subject_id    BIGINT,
    pc_status             INT,
    pc_created_time       TIMESTAMP,
    pc_deleted_time       TIMESTAMP,
    pc_updated_time       TIMESTAMP,
    pc_dw_created_time    TIMESTAMP,
    pc_dw_updated_time    TIMESTAMP
);

CREATE TABLE alefdw.dim_pathway_curriculum
(
    pc_dw_id              BIGINT IDENTITY(1,1),
    pc_pathway_dw_id      BIGINT,
    pc_pathway_id         VARCHAR(36),
    pc_curr_id            BIGINT,
    pc_curr_grade_id      BIGINT,
    pc_curr_subject_id    BIGINT,
    pc_status             INT,
    pc_created_time       TIMESTAMP,
    pc_deleted_time       TIMESTAMP,
    pc_updated_time       TIMESTAMP,
    pc_dw_created_time    TIMESTAMP,
    pc_dw_updated_time    TIMESTAMP
);

CREATE TABLE alefdw_stage.rel_pathway_content_repository
(
    rel_pcr_dw_id          BIGINT IDENTITY(1,1),
    pcr_pathway_id         VARCHAR(36),
    pcr_repository_id      VARCHAR(36),
    pcr_status             INT,
    pcr_created_time       TIMESTAMP,
    pcr_deleted_time       TIMESTAMP,
    pcr_updated_time       TIMESTAMP,
    pcr_dw_created_time    TIMESTAMP,
    pcr_dw_updated_time    TIMESTAMP
);

CREATE TABLE alefdw.dim_pathway_content_repository
(
    pcr_dw_id               BIGINT IDENTITY(1,1),
    pcr_pathway_dw_id       BIGINT,
    pcr_pathway_id          VARCHAR(36),
    pcr_repository_dw_id    BIGINT,
    pcr_repository_id       VARCHAR(36),
    pcr_status              INT,
    pcr_created_time        TIMESTAMP,
    pcr_deleted_time        TIMESTAMP,
    pcr_updated_time        TIMESTAMP,
    pcr_dw_created_time     TIMESTAMP,
    pcr_dw_updated_time     TIMESTAMP
);

ALTER TABLE alefdw_stage.rel_pathway_level_activity_association ADD COLUMN plaa_is_joint_parent_activity BOOLEAN;
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_is_joint_parent_activity BOOLEAN;


CREATE TABLE alefdw_stage.rel_plaa_outcome
(
    rel_plaa_outcome_dw_id          BIGINT IDENTITY (1, 1),
    plaa_outcome_pathway_id         VARCHAR(36),
    plaa_outcome_activity_id        VARCHAR(36),
    plaa_outcome_id                 BIGINT,
    plaa_outcome_type               VARCHAR(50),
    plaa_outcome_curr_id            BIGINT,
    plaa_outcome_curr_grade_id      BIGINT,
    plaa_outcome_curr_subject_id    BIGINT,
    plaa_outcome_status             INT,
    plaa_outcome_attach_status      INT,
    plaa_outcome_created_time       TIMESTAMP,
    plaa_outcome_updated_time       TIMESTAMP,
    plaa_outcome_dw_created_time    TIMESTAMP,
    plaa_outcome_dw_updated_time    TIMESTAMP
);

CREATE TABLE alefdw.dim_plaa_outcome
(
    plaa_outcome_dw_id              BIGINT IDENTITY (1, 1),
    plaa_outcome_pathway_dw_id      BIGINT,
    plaa_outcome_pathway_id         VARCHAR(36),
    plaa_outcome_activity_dw_id     BIGINT,
    plaa_outcome_activity_id        VARCHAR(36),
    plaa_outcome_id                 BIGINT,
    plaa_outcome_type               VARCHAR(50),
    plaa_outcome_curr_id            BIGINT,
    plaa_outcome_curr_grade_id      BIGINT,
    plaa_outcome_curr_subject_id    BIGINT,
    plaa_outcome_status             INT,
    plaa_outcome_attach_status      INT,
    plaa_outcome_created_time       TIMESTAMP,
    plaa_outcome_updated_time       TIMESTAMP,
    plaa_outcome_dw_created_time    TIMESTAMP,
    plaa_outcome_dw_updated_time    TIMESTAMP
);


CREATE TABLE alefdw.fact_service_desk_request
(
    fsdr_dw_id BIGINT IDENTITY(1, 1),
    fsdr_dw_created_time TIMESTAMP,
    fsdr_created_time TIMESTAMP,
    fsdr_completed_time TIMESTAMP,
    fsdr_category_name varchar(50),
    fsdr_sub_category varchar(50),
    fsdr_subject varchar(300),
    fsdr_request_status varchar(30),
    fsdr_request_type varchar(30),
    fsdr_request_id  BIGINT,
    fsdr_item_name varchar(30),
    fsdr_site_name varchar(50),
    fsdr_group_name varchar(50),
    fsdr_technician_name varchar(50),
    fsdr_template_name varchar(100),
    fsdr_closure_code_name varchar(50),
    fsdr_impact_name varchar(50),
    fsdr_resolved_time TIMESTAMP,
    fsdr_assigned_time TIMESTAMP,
    fsdr_sla_name varchar(50),
    fsdr_impact_details_name varchar(50),
    fsdr_linked_request_id BIGINT,
    fsdr_organization_name varchar(30),
    fsdr_is_escalated boolean,
    fsdr_priority_name varchar(50),
    fsdr_request_display_id BIGINT,
    fsdr_request_mode_name varchar(50),
    fsdr_requester_job_title varchar(200),
    fsdr_date_dw_id BIGINT
)
    DISTSTYLE KEY DISTKEY (fsdr_dw_id)
    SORTKEY (fsdr_created_time);

ALTER TABLE alefdw_stage.staging_adt_student_report RENAME tenant_uuid to fasr_tenant_id;
ALTER TABLE alefdw_stage.staging_adt_student_report RENAME student_uuid to fasr_student_id;
ALTER TABLE alefdw_stage.staging_adt_student_report RENAME fle_ls_uuid to fasr_fle_ls_id;
ALTER TABLE alefdw_stage.staging_adt_student_report RENAME school_uuid to fasr_school_id;

CREATE TABLE alefdw.fact_student_certificate_awarded(
    fsca_dw_id BIGINT IDENTITY(1, 1),
    fsca_created_time TIMESTAMP,
    fsca_dw_created_time TIMESTAMP,
    fsca_date_dw_id BIGINT,
    fsca_certificate_id VARCHAR(36),
    fsca_student_dw_id BIGINT,
    fsca_award_category VARCHAR(50),
    fsca_award_purpose VARCHAR(100),
    fsca_academic_year_dw_id BIGINT,
    fsca_class_dw_id BIGINT,
    fsca_grade_dw_id BIGINT,
    fsca_teacher_dw_id BIGINT,
    fsca_language VARCHAR(36),
    fsca_tenant_dw_id BIGINT
)
DISTSTYLE KEY DISTKEY (fsca_dw_id)
SORTKEY (fsca_created_time);


CREATE TABLE alefdw_stage.staging_student_certificate_awarded(
    fsca_dw_id BIGINT IDENTITY(1, 1),
    fsca_created_time TIMESTAMP,
    fsca_dw_created_time TIMESTAMP,
    fsca_date_dw_id BIGINT,
    fsca_certificate_id VARCHAR(36),
    fsca_student_id VARCHAR(36),
    fsca_award_category VARCHAR(50),
    fsca_award_purpose VARCHAR(100),
    fsca_academic_year_id VARCHAR(36),
    fsca_class_id VARCHAR(36),
    fsca_grade_id VARCHAR(36),
    fsca_teacher_id VARCHAR(36),
    fsca_language VARCHAR(36),
    fsca_tenant_id VARCHAR(36)
)
SORTKEY (fsca_dw_id);

ALTER TABLE alefdw_stage.rel_pathway DROP COLUMN pathway_content_repository;
ALTER TABLE alefdw.dim_pathway DROP COLUMN pathway_content_repository_dw_id;


ALTER TABLE alefdw.fact_service_desk_request ALTER COLUMN fsdr_site_name TYPE VARCHAR(100);

DROP TABLE alefdw.fact_user_heartbeat_aggregated;
DROP TABLE alefdw_stage.staging_user_heartbeat_aggregated;

CREATE TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated(
    fuhha_staging_id BIGINT IDENTITY(1, 1),
    fuhha_created_time TIMESTAMP,
    fuhha_dw_created_time TIMESTAMP,
    fuhha_date_dw_id BIGINT,
    fuhha_user_dw_id BIGINT,
    fuhha_role VARCHAR(36),
    fuhha_school_dw_id BIGINT,
    fuhha_content_repository_dw_id BIGINT,
    fuhha_channel VARCHAR(36),
    fuhha_activity_date_hour TIMESTAMP,
    fuhha_tenant_id VARCHAR(36)
)
SORTKEY (fuhha_staging_id);

CREATE TABLE alefdw.fact_user_heartbeat_hourly_aggregated(
    fuhha_dw_id BIGINT IDENTITY(1, 1),
    fuhha_created_time TIMESTAMP,
    fuhha_dw_created_time TIMESTAMP,
    fuhha_date_dw_id BIGINT,
    fuhha_user_dw_id BIGINT,
    fuhha_role_dw_id BIGINT,
    fuhha_school_dw_id BIGINT,
    fuhha_content_repository_dw_id BIGINT,
    fuhha_channel VARCHAR(36),
    fuhha_activity_date_hour TIMESTAMP,
    fuhha_tenant_dw_id BIGINT
)
DISTSTYLE KEY DISTKEY (fuhha_dw_id)
SORTKEY (fuhha_created_time);

CREATE TABLE alefdw.fact_jira_issue
(
    fji_dw_id BIGINT IDENTITY(1, 1),
    fji_resolution varchar(100),
    fji_issue_type varchar(100),
    fji_assignee varchar(50),
    fji_reporter varchar(50),
    fji_priority varchar(20),
    fji_status varchar(50),
    fji_original_estimate BIGINT,
    fji_percent_of_spent_time varchar(30),
    fji_labels  varchar(100),
    fji_summary varchar(500),
    fji_key varchar(50),
    fji_due_date TIMESTAMP,
    fji_date_dw_id BIGINT,
    fji_created_time TIMESTAMP,
    fji_dw_created_time TIMESTAMP
);

ALTER TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated DROP COLUMN fuhha_user_dw_id;
ALTER TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated DROP COLUMN fuhha_school_dw_id;
ALTER TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated DROP COLUMN fuhha_content_repository_dw_id;
ALTER TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated ADD COLUMN fuhha_user_id VARCHAR(36);


ALTER TABLE alefdw_stage.rel_pathway_level ADD COLUMN pathway_longname VARCHAR(255);
ALTER TABLE alefdw.dim_pathway_level ADD COLUMN pathway_longname VARCHAR(255);

CREATE TABLE alefdw_stage.staging_guardian_joint_activity
(
    fgja_staging_id BIGINT IDENTITY(1, 1),
    fgja_created_time TIMESTAMP,
    fgja_dw_created_time TIMESTAMP,
    fgja_date_dw_id BIGINT,
    fgja_tenant_id VARCHAR,
    fgja_school_id VARCHAR,
    fgja_k12_grade INTEGER,
    fgja_class_id VARCHAR,
    fgja_student_id VARCHAR,
    fgja_guardian_id VARCHAR,
    fgja_pathway_id VARCHAR,
    fgja_pathway_level_id VARCHAR,
    fgja_attempt Smallint,
    fgja_rating Smallint,
    fgja_state Smallint,
    fgja_updated_time TIMESTAMP
)
SORTKEY (fgja_staging_id);

CREATE TABLE alefdw.fact_guardian_joint_activity
(
    fgja_dw_id BIGINT IDENTITY(1, 1),
    fgja_created_time TIMESTAMP,
    fgja_dw_created_time TIMESTAMP,
    fgja_date_dw_id BIGINT,
    fgja_tenant_dw_id BIGINT,
    fgja_school_dw_id BIGINT,
    fgja_k12_grade INTEGER,
    fgja_class_dw_id BIGINT,
    fgja_student_dw_id BIGINT,
    fgja_guardian_dw_id BIGINT,
    fgja_pathway_dw_id BIGINT,
    fgja_pathway_level_dw_id BIGINT,
    fgja_attempt Smallint,
    fgja_rating Smallint,
    fgja_state Smallint,
    fgja_updated_time TIMESTAMP
)
DISTSTYLE KEY DISTKEY (fgja_dw_id)
SORTKEY (fgja_created_time);

ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_total_stars Smallint;
ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_total_stars Smallint;


ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_session_state INT;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_session_state INT;

CREATE TABLE alefdw_stage.staging_user_avatar (
    fua_dw_id BIGINT,
    fua_created_time TIMESTAMP,
    fua_dw_created_time TIMESTAMP,
    fua_date_dw_id BIGINT,
    fua_id VARCHAR(255),
    fua_user_id VARCHAR(36),
    fua_tenant_id VARCHAR(36),
    fua_school_id VARCHAR(36),
    fua_grade_id VARCHAR(36)
);


CREATE TABLE alefdw.fact_user_avatar
(
    fua_dw_id BIGINT,
    fua_created_time TIMESTAMP,
    fua_dw_created_time TIMESTAMP,
    fua_date_dw_id BIGINT,
    fua_id VARCHAR(255),
    fua_user_id VARCHAR(36),
    fua_user_dw_id BIGINT,
    fua_tenant_id VARCHAR(36),
    fua_tenant_dw_id BIGINT,
    fua_school_id VARCHAR(36),
    fua_school_dw_id BIGINT,
    fua_grade_id VARCHAR(36),
    fua_grade_dw_id BIGINT
);

ALTER TABLE alefdw.fact_jira_issue alter COLUMN fji_labels type VARCHAR(255);
ALTER TABLE alefdw.fact_jira_issue alter COLUMN fji_summary type VARCHAR(255);
ALTER TABLE alefdw.fact_jira_issue add COLUMN fji_issue_created_on TIMESTAMP;

ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_forecast_score FLOAT;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_forecast_score FLOAT;

ALTER TABLE alefdw_stage.rel_content_repository RENAME COLUMN content_repository_uuid TO content_repository_id;
ALTER TABLE alefdw_stage.rel_content_repository RENAME COLUMN content_repository_organization TO content_repository_organisation_owner;
ALTER TABLE alefdw.dim_content_repository RENAME COLUMN content_repository_organization TO content_repository_organisation_owner;

CREATE TABLE alefdw_stage.staging_pathway_placement
(
    fpp_staging_id BIGINT IDENTITY(1, 1),
    fpp_created_time TIMESTAMP,
    fpp_dw_created_time TIMESTAMP,
    fpp_date_dw_id BIGINT,
    fpp_previous_pathway_domain VARCHAR(100),
    fpp_pathway_id VARCHAR(36),
    fpp_new_pathway_domain VARCHAR(100),
    fpp_new_pathway_grade INTEGER,
    fpp_class_id VARCHAR(36),
    fpp_student_id VARCHAR(36),
    fpp_previous_pathway_grade INTEGER,
    fpp_tenant_id VARCHAR(36),
    fpp_placement_type INTEGER,
    fpp_overall_grade INTEGER,
    fpp_created_by VARCHAR(36)
)
DISTSTYLE KEY DISTKEY (fpp_staging_id)
SORTKEY (fpp_created_time);


CREATE TABLE alefdw.fact_pathway_placement
(
    fpp_dw_id BIGINT IDENTITY(1, 1),
    fpp_created_time TIMESTAMP,
    fpp_dw_created_time TIMESTAMP,
    fpp_date_dw_id BIGINT,
    fpp_previous_pathway_domain VARCHAR(100),
    fpp_pathway_id VARCHAR(36),
    fpp_pathway_dw_id BIGINT,
    fpp_new_pathway_domain VARCHAR(100),
    fpp_new_pathway_grade INTEGER,
    fpp_class_id VARCHAR(36),
    fpp_class_dw_id BIGINT,
    fpp_student_id VARCHAR(36),
    fpp_student_dw_id BIGINT,
    fpp_previous_pathway_grade INTEGER,
    fpp_tenant_id VARCHAR(36),
    fpp_tenant_dw_id BIGINT,
    fpp_placement_type INTEGER,
    fpp_overall_grade INTEGER,
    fpp_created_by VARCHAR(36),
    fpp_created_by_dw_id BIGINT
)
    DISTSTYLE KEY DISTKEY (fpp_dw_id)
SORTKEY (fpp_created_time);

ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_grade VARCHAR(36);
ALTER TABLE alefdw_stage.rel_pathway_level_activity_association ADD COLUMN plaa_grade VARCHAR(36);

ALTER TABLE alefdw_stage.rel_pathway_level_activity_association ADD COLUMN plaa_activity_is_optional BOOLEAN;
ALTER TABLE alefdw.dim_pathway_level_activity_association ADD COLUMN plaa_activity_is_optional BOOLEAN;
ALTER TABLE alefdw_stage.staging_tutor_user_context ADD COLUMN ftc_tutor_locked BOOLEAN;
ALTER TABLE alefdw.fact_tutor_user_context ADD COLUMN ftc_tutor_locked BOOLEAN;

ALTER TABLE alefdw_stage.staging_tutor_session ADD COLUMN fts_session_message_limit_reached BOOLEAN;
ALTER TABLE alefdw.fact_tutor_session ADD COLUMN fts_session_message_limit_reached BOOLEAN;

ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_message_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_conversation_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_conversation_token_count INT;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_suggestions_prompt_tokens VARCHAR;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_message_tokens VARCHAR;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_activity_page_context_id VARCHAR;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_student_location VARCHAR;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_suggestion_clicked BOOLEAN;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_clicked_suggestion_id VARCHAR(36);
ALTER TABLE alefdw_stage.staging_tutor_conversation DROP COLUMN ftc_message_feedback;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_message_feedback INTEGER;

ALTER TABLE alefdw_stage.staging_tutor_conversation DROP COLUMN ftc_bot_suggestions_response_time;
ALTER TABLE alefdw_stage.staging_tutor_conversation DROP COLUMN ftc_bot_suggestions_confidence;


ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_message_id VARCHAR(36);
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_conversation_id VARCHAR(36);
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_conversation_token_count INT;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_suggestions_prompt_tokens VARCHAR;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_message_tokens VARCHAR;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_activity_page_context_id VARCHAR;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_student_location VARCHAR;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_suggestion_clicked BOOLEAN;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_clicked_suggestion_id VARCHAR(36);
ALTER TABLE alefdw.fact_tutor_conversation DROP COLUMN ftc_message_feedback;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_message_feedback INTEGER;

ALTER TABLE alefdw.fact_tutor_conversation DROP COLUMN ftc_bot_suggestions_response_time;
ALTER TABLE alefdw.fact_tutor_conversation DROP COLUMN ftc_bot_suggestions_confidence;

CREATE TABLE alefdw_stage.staging_tutor_suggestions
(
    fts_dw_id	bigint
    ,fts_message_Id	VARCHAR(36)
    ,fts_suggestion_id	VARCHAR(36)
    ,fts_created_time	timestamp
    ,fts_dw_created_time	timestamp
    ,fts_user_Id	VARCHAR(36)
    ,fts_session_Id	VARCHAR(36)
    ,fts_conversation_Id	VARCHAR(36)
    ,fts_response_time	FLOAT
    ,fts_success_parser_tokens	integer
    ,fts_failure_parser_tokens	integer
    ,fts_suggestion_clicked	boolean
    ,fts_date_dw_id BIGINT
    ,fts_tenant_id VARCHAR(36)
);

CREATE TABLE alefdw.fact_tutor_suggestions
(
    fts_dw_id	bigint
    ,fts_message_Id	VARCHAR(36)
    ,fts_suggestion_id	VARCHAR(36)
    ,fts_created_time	timestamp
    ,fts_dw_created_time	timestamp
    ,fts_user_Id	VARCHAR(36)
    ,fts_user_dw_id BIGINT
    ,fts_session_Id	VARCHAR(36)
    ,fts_conversation_Id	VARCHAR(36)
    ,fts_response_time	FLOAT
    ,fts_success_parser_tokens	integer
    ,fts_failure_parser_tokens	integer
    ,fts_suggestion_clicked	boolean
    ,fts_date_dw_id BIGINT
    ,fts_tenant_dw_id BIGINT
);

alter table alefdw_stage.rel_dw_id_mappings add column entity_dw_created_time timestamp;

create table alefdw_stage.rel_content_repository_material_association (
  rel_crma_dw_id bigint identity(1,1),
  crma_content_repository_id varchar(36),
  crma_material_id varchar(36),
  crma_status int,
  crma_type int,
  crma_attach_status int,
  crma_created_time timestamp,
  crma_dw_created_time timestamp,
  crma_updated_time timestamp,
  crma_dw_updated_time timestamp
)  DISTSTYLE KEY DISTKEY (rel_crma_dw_id)
SORTKEY (crma_created_time);

create table alefdw.dim_content_repository_material_association (
    crma_dw_id bigint identity(1,1),
    crma_content_repository_id varchar(36),
    crma_content_repository_dw_id bigint,
    crma_material_id varchar(36),
    crma_material_dw_id bigint,
    crma_status int,
    crma_type int,
    crma_attach_status int,
    crma_created_time timestamp,
    crma_dw_created_time timestamp,
    crma_updated_time timestamp,
    crma_dw_updated_time timestamp
) DISTSTYLE KEY DISTKEY (crma_dw_id)
SORTKEY (crma_created_time);

ALTER TABLE alefdw_stage.staging_tutor_conversation DROP COLUMN ftc_message_feedback;
ALTER TABLE alefdw_stage.staging_tutor_conversation ADD COLUMN ftc_message_feedback VARCHAR(10);
ALTER TABLE alefdw.fact_tutor_conversation DROP COLUMN ftc_message_feedback;
ALTER TABLE alefdw.fact_tutor_conversation ADD COLUMN ftc_message_feedback VARCHAR(10);


ALTER TABLE alefdw.fact_tutor_suggestions ADD COLUMN fts_tenant_id VARCHAR(36);

alter table alefdw_stage.rel_dw_id_mappings add column entity_created_time timestamp;
alter table alefdw_stage.rel_user add column user_dw_created_time timestamp;

ALTER TABLE alefdw_stage.rel_question RENAME COLUMN question_uuid TO question_id;

ALTER TABLE alefdw_stage.rel_guardian RENAME COLUMN guardian_uuid TO guardian_id;
ALTER TABLE alefdw_stage.rel_guardian RENAME COLUMN student_uuid TO student_id;

-- Remove Identity column and create plain bigint column
-- !!! Make sure the staging table is empty !!!
DROP TABLE alefdw_stage.rel_guardian;
CREATE TABLE alefdw_stage.rel_guardian
(
  rel_guardian_dw_id            BIGINT,
  guardian_created_time         TIMESTAMP,
  guardian_updated_time         TIMESTAMP,
  guardian_deleted_time         TIMESTAMP,
  guardian_dw_created_time      TIMESTAMP,
  guardian_dw_updated_time      TIMESTAMP,
  guardian_active_until         TIMESTAMP,
  guardian_status               INT,
  guardian_id                   VARCHAR(36),
  student_id                    VARCHAR(36),
  guardian_invitation_status    INT
)
compound sortkey(student_id);

CREATE TABLE alefdw.dim_guardian_copy
(
  rel_guardian_dw_id            BIGINT,
  guardian_created_time         TIMESTAMP,
  guardian_updated_time         TIMESTAMP,
  guardian_deleted_time         TIMESTAMP,
  guardian_dw_created_time      TIMESTAMP,
  guardian_dw_updated_time      TIMESTAMP,
  guardian_active_until         TIMESTAMP,
  guardian_status               INT,
  guardian_id                   VARCHAR(36),
  guardian_dw_id                BIGINT,
  guardian_student_dw_id        BIGINT,
  guardian_invitation_status    INT
)
distkey(guardian_student_dw_id)
compound sortkey(guardian_student_dw_id);

INSERT INTO alefdw.dim_guardian_copy
SELECT
  rel_guardian_dw_id,
  guardian_created_time,
  guardian_updated_time,
  guardian_deleted_time,
  guardian_dw_created_time,
  guardian_dw_updated_time,
  guardian_active_until,
  guardian_status,
  guardian_id,
  guardian_dw_id,
  guardian_student_dw_id,
  guardian_invitation_status
FROM alefdw.dim_guardian;

CREATE TABLE backups.dim_guardian_2023_11_30 AS SELECT * FROM alefdw.dim_guardian;
DROP TABLE alefdw.dim_guardian;
ALTER TABLE alefdw.dim_guardian_copy RENAME TO dim_guardian;

ALTER TABLE alefdw_stage.rel_teacher RENAME COLUMN teacher_uuid TO teacher_id;
ALTER TABLE alefdw_stage.rel_teacher RENAME COLUMN school_uuid TO school_id;
ALTER TABLE alefdw_stage.rel_teacher RENAME COLUMN subject_uuid TO subject_id;

ALTER TABLE alefdw_stage.staging_learning_experience add column fle_open_path_enabled BOOLEAN default false;
ALTER TABLE alefdw.fact_learning_experience add column fle_open_path_enabled BOOLEAN default false;

ALTER TABLE alefdw_stage.staging_experience_submitted add column fes_open_path_enabled BOOLEAN default false;
ALTER TABLE alefdw.fact_experience_submitted add column fes_open_path_enabled BOOLEAN default false;


-- Remove Identity column and create a plain bigint column
CREATE TABLE backups.dim_tag_2023_12_20 AS SELECT * FROM alefdw.dim_tag;
ALTER TABLE alefdw.dim_tag ADD COLUMN tag_dw_id_copy BIGINT;
UPDATE alefdw.dim_tag SET tag_dw_id_copy = tag_dw_id;
ALTER TABLE alefdw.dim_tag ALTER SORTKEY NONE;
ALTER TABLE alefdw.dim_tag DROP COLUMN tag_dw_id;
ALTER TABLE alefdw.dim_tag RENAME COLUMN tag_dw_id_copy TO tag_dw_id;
ALTER TABLE alefdw.dim_tag ALTER COMPOUND SORTKEY (tag_dw_id);


ALTER TABLE alefdw.dim_content_repository_material_association DROP COLUMN crma_material_dw_id;

create table alefdw.dim_course
(
    rel_course_dw_id          bigint,
    course_dw_id              bigint ,
    course_id                 varchar(36),
    course_type               varchar(25),
    course_status             integer ,
    course_name               varchar(255),
    course_code               varchar(50),
    course_subject_id         integer ,
    course_organization_dw_id bigint ,
    course_created_time       timestamp ,
    course_deleted_time       timestamp ,
    course_updated_time       timestamp ,
    course_dw_created_time    timestamp ,
    course_dw_updated_time    timestamp ,
    course_dw_deleted_time    timestamp ,
    course_lang_code          varchar(10)
);

create table alefdw.dim_course_content_repository
(
    ccr_dw_id            bigint,
    ccr_course_dw_id    bigint ,
    ccr_course_id       varchar(36),
    ccr_repository_dw_id bigint ,
    ccr_repository_id    varchar(36),
    ccr_status           integer ,
    ccr_created_time     timestamp ,
    ccr_updated_time     timestamp ,
    ccr_deleted_time     timestamp ,
    ccr_dw_created_time  timestamp ,
    ccr_dw_updated_time  timestamp
);

create table alefdw.dim_course_curriculum
(
    cc_dw_id           bigint,
    cc_course_dw_id   bigint ,
    cc_course_id      varchar(36),
    cc_curr_id         bigint ,
    cc_curr_grade_id   bigint ,
    cc_curr_subject_id bigint ,
    cc_status          integer ,
    cc_created_time    timestamp ,
    cc_updated_time    timestamp ,
    cc_deleted_time    timestamp ,
    cc_dw_created_time timestamp ,
    cc_dw_updated_time timestamp
);

create table alefdw_stage.rel_course
(
    rel_course_dw_id       bigint,
    course_id              varchar(36),
    course_status          integer ,
    course_name            varchar(255),
    course_type            varchar(25),
    course_code            varchar(50),
    course_subject_id      integer ,
    course_organization    varchar(50),
    course_created_time    timestamp ,
    course_deleted_time    timestamp ,
    course_updated_time    timestamp ,
    course_dw_created_time timestamp ,
    course_dw_updated_time timestamp ,
    course_dw_deleted_time timestamp ,
    course_lang_code       varchar(10)
);

create table alefdw_stage.rel_course_content_repository
(
    ccr_dw_id       bigint,
    ccr_course_id      varchar(36),
    ccr_repository_id   varchar(36),
    ccr_status          integer ,
    ccr_created_time    timestamp ,
    ccr_updated_time    timestamp ,
    ccr_deleted_time    timestamp ,
    ccr_dw_created_time timestamp ,
    ccr_dw_updated_time timestamp
);

create table alefdw_stage.rel_course_curriculum
(
    cc_dw_id       bigint,
    cc_course_id      varchar(36),
    cc_curr_id         bigint ,
    cc_curr_grade_id   bigint ,
    cc_curr_subject_id bigint ,
    cc_status          integer ,
    cc_created_time    timestamp ,
    cc_updated_time    timestamp ,
    cc_deleted_time    timestamp ,
    cc_dw_created_time timestamp ,
    cc_dw_updated_time timestamp
);

--dim_curriculum dw_id DDL Updates
CREATE TABLE backups.dim_curriculum_20230108 AS SELECT * FROM alefdw.dim_curriculum;
ALTER TABLE alefdw.dim_curriculum ADD COLUMN curr_dw_id_copy BIGINT;
UPDATE alefdw.dim_curriculum SET curr_dw_id_copy = curr_dw_id;
ALTER TABLE alefdw.dim_curriculum ALTER SORTKEY NONE;
ALTER TABLE alefdw.dim_curriculum DROP COLUMN curr_dw_id;
ALTER TABLE alefdw.dim_curriculum RENAME COLUMN curr_dw_id_copy TO curr_dw_id;
ALTER TABLE alefdw.dim_curriculum ALTER COMPOUND SORTKEY (curr_dw_id);


--staging_user_heartbeat_hourly_aggregated school_id DDL updates
ALTER TABLE alefdw_stage.staging_user_heartbeat_hourly_aggregated ADD COLUMN fuhha_school_id varchar(36);


--dim_class dw_id DDL Updates
-- !!! Make sure the staging table is empty !!!
ALTER TABLE alefdw_stage.rel_class DROP COLUMN rel_class_id;
ALTER TABLE alefdw_stage.rel_class ADD COLUMN rel_class_dw_id BIGINT;

CREATE TABLE backups.dim_class_20230110 AS SELECT * FROM alefdw.dim_class;
ALTER TABLE alefdw.dim_class ADD COLUMN rel_class_dw_id_copy BIGINT;
UPDATE alefdw.dim_class SET rel_class_dw_id_copy = rel_class_dw_id WHERE 1 = 1;
ALTER TABLE alefdw.dim_class ALTER SORTKEY NONE;
ALTER TABLE alefdw.dim_class DROP COLUMN rel_class_dw_id;
ALTER TABLE alefdw.dim_class RENAME COLUMN rel_class_dw_id_copy TO rel_class_dw_id;
ALTER TABLE alefdw.dim_class ALTER COMPOUND SORTKEY (rel_class_dw_id);

--dim_class_user dw_id DDL Updates
-- !!! Make sure the staging table is empty !!!
ALTER TABLE alefdw_stage.rel_class_user DROP COLUMN rel_class_user_id;
ALTER TABLE alefdw_stage.rel_class_user ADD COLUMN rel_class_user_dw_id BIGINT;

CREATE TABLE backups.dim_class_user_20230110 AS SELECT * FROM alefdw.dim_class_user;
ALTER TABLE alefdw.dim_class_user ADD COLUMN rel_class_user_dw_id_copy BIGINT;
UPDATE alefdw.dim_class_user SET rel_class_user_dw_id_copy = rel_class_user_dw_id WHERE 1 = 1;
ALTER TABLE alefdw.dim_class_user ALTER SORTKEY NONE;
ALTER TABLE alefdw.dim_class_user DROP COLUMN rel_class_user_dw_id;
ALTER TABLE alefdw.dim_class_user RENAME COLUMN rel_class_user_dw_id_copy TO rel_class_user_dw_id;
ALTER TABLE alefdw.dim_class_user ALTER COMPOUND SORTKEY (rel_class_user_dw_id);


ALTER TABLE alefdw_stage.fact_level_completed ADD COLUMN flc_course_dw_id bigint;
ALTER TABLE alefdw_stage.fact_levels_recommended ADD COLUMN flr_course_dw_id bigint;
ALTER TABLE alefdw_stage.fact_pathway_placement ADD COLUMN fpp_course_dw_id bigint;
ALTER TABLE alefdw_stage.fact_pathway_leaderboard ADD COLUMN fpl_course_dw_id bigint;
ALTER TABLE alefdw_stage.fact_pathway_activity_completed ADD COLUMN fpac_course_dw_id bigint;
ALTER TABLE alefdw_stage.fact_guardian_joint_activity ADD COLUMN fgja_course_dw_id bigint;

create table alefdw_stage.rel_course_activity_container
    (
        course_activity_container_is_accelerated boolean,
        course_activity_container_pacing varchar(50),
        course_activity_container_sequence INT,
        course_activity_container_course_version VARCHAR(10),
        course_activity_container_attach_status INT,
        course_activity_container_id VARCHAR(36),
        course_activity_container_longname VARCHAR(255),
        course_activity_container_updated_time TIMESTAMP ,
        course_activity_container_dw_created_time TIMESTAMP,
        course_activity_container_title varchar(50),
        course_activity_container_created_time TIMESTAMP,
        course_activity_container_course_id VARCHAR(36),
        course_activity_container_domain varchar(50),
        course_activity_container_status INT,
        course_activity_container_dw_updated_time TIMESTAMP,
        course_activity_container_index INT,
        rel_course_activity_container_dw_id BIGINT
);

create table alefdw.dim_course_activity_container
(
    course_activity_container_is_accelerated boolean,
    course_activity_container_pacing varchar(50),
    course_activity_container_sequence INT,
    course_activity_container_course_version VARCHAR(10),
    course_activity_container_attach_status INT,
    course_activity_container_id VARCHAR(36),
    course_activity_container_dw_id BIGINT,
    course_activity_container_longname VARCHAR(255),
    course_activity_container_updated_time TIMESTAMP ,
    course_activity_container_dw_created_time TIMESTAMP,
    course_activity_container_title varchar(50),
    course_activity_container_created_time TIMESTAMP,
    course_activity_container_course_id VARCHAR(36),
    course_activity_container_domain varchar(50),
    course_activity_container_status INT,
    course_activity_container_dw_updated_time TIMESTAMP,
    course_activity_container_index INT,
    rel_course_activity_container_dw_id BIGINT
);

create table alefdw_stage.rel_course_activity_container_grade_association
(
    cacga_dw_id BIGINT,
    cacga_container_id VARCHAR(36),
    cacga_course_id VARCHAR(36),
    cacga_grade VARCHAR(10),
    cacga_created_time TIMESTAMP,
    cacga_dw_created_time TIMESTAMP,
    cacga_updated_time TIMESTAMP,
    cacga_dw_updated_time TIMESTAMP,
    cacga_status INT
);

create table alefdw.dim_course_activity_container_grade_association
(
    cacga_dw_id BIGINT,
    cacga_container_id VARCHAR(36),
    cacga_container_dw_id BIGINT,
    cacga_course_id VARCHAR(36),
    cacga_course_dw_id BIGINT,
    cacga_grade VARCHAR(10),
    cacga_created_time TIMESTAMP,
    cacga_dw_created_time TIMESTAMP,
    cacga_updated_time TIMESTAMP,
    cacga_dw_updated_time TIMESTAMP,
    cacga_status INT
);

ALTER TABLE alefdw_stage.rel_course_activity_container DROP COLUMN course_activity_container_grade;
ALTER TABLE alefdw.dim_course_activity_container DROP COLUMN course_activity_container_grade;

alter table alefdw.fact_pathway_activity_completed add column fpac_course_activity_container_dw_id bigint;
alter table alefdw.fact_tutor_session add column fts_course_activity_container_dw_id bigint;
alter table alefdw.fact_tutor_conversation add column ftc_course_activity_container_dw_id bigint;
alter table alefdw.fact_guardian_joint_activity add column fgja_course_activity_container_dw_id bigint;
alter table alefdw.fact_level_completed add column flc_course_activity_container_dw_id bigint;
alter table alefdw.fact_levels_recommended add column flr_course_activity_container_dw_id bigint;
alter table alefdw.fact_levels_recommended add column flr_completed_course_activity_container_dw_id bigint;

create table alefdw_stage.rel_pathway_grade_association
(
    pg_dw_id           bigint,
    pg_pathway_dw_id   bigint,
    pg_pathway_id      varchar(36),
    pg_grade_id        int,
    pg_status          int,
    pg_created_time    TIMESTAMP,
    pg_dw_created_time TIMESTAMP,
    pg_updated_time    TIMESTAMP,
    pg_dw_updated_time TIMESTAMP
)

create table alefdw.dim_pathway_grade_association
(
    pg_dw_id           bigint,
    pg_pathway_dw_id   bigint,
    pg_pathway_id      varchar(36),
    pg_grade_id        int,
    pg_grade_dw_id     bigint,
    pg_status          int,
    pg_created_time    TIMESTAMP,
    pg_dw_created_time TIMESTAMP,
    pg_updated_time    TIMESTAMP,
    pg_dw_updated_time TIMESTAMP
)
foreign key(pg_grade_dw_id) references alefdw_stage.rel_dw_id_mappings(entity_dw_id),
foreign key(pg_pathway_dw_id) references alefdw.dim_grade(grade_dw_id)

create table alefdw_stage.rel_course_grade_association
(
    cg_dw_id           bigint,
    cg_course_id       varchar(36),
    cg_grade_id        int,
    cg_status          int,
    cg_created_time    TIMESTAMP,
    cg_dw_created_time TIMESTAMP,
    cg_updated_time    TIMESTAMP,
    cg_dw_updated_time TIMESTAMP
)

create table alefdw.dim_course_grade_association
(
    cg_dw_id           bigint,
    cg_course_dw_id    bigint,
    cg_course_id       varchar(36),
    cg_grade_id        int,
    cg_status          int,
    cg_grade_dw_id    bigint,
    cg_created_time    TIMESTAMP,
    cg_dw_created_time TIMESTAMP,
    cg_updated_time    TIMESTAMP,
    cg_dw_updated_time TIMESTAMP
)
foreign key(cg_course_dw_id) references alefdw_stage.rel_dw_id_mappings(dw_id),
foreign key(cg_grade_dw_id) references alefdw.dim_grade(grade_dw_id)

create table alefdw_stage.rel_course_subject_association
(
    cs_dw_id           bigint,
    cs_course_id       varchar(36),
    cs_subject_id      int,
    cs_status          int,
    cs_created_time    TIMESTAMP,
    cs_dw_created_time TIMESTAMP,
    cs_updated_time    TIMESTAMP,
    cs_dw_updated_time TIMESTAMP
)

create table alefdw.dim_course_subject_association
(
    cs_dw_id           bigint,
    cs_course_dw_id    bigint,
    cs_course_id       varchar(36),
    cs_subject_dw_id    bigint,
    cs_subject_id      int,
    cs_status          int,
    cs_created_time    TIMESTAMP,
    cs_dw_created_time TIMESTAMP,
    cs_updated_time    TIMESTAMP,
    cs_dw_updated_time TIMESTAMP
)
foreign key(cs_course_dw_id) references alefdw_stage.rel_dw_id_mappings(dw_id),

create table alefdw_stage.rel_pathway_subject_association
(
    ps_dw_id           bigint,
    ps_pathway_id      varchar(36),
    ps_subject_id      int,
    ps_status          int,
    ps_created_time    TIMESTAMP,
    ps_dw_created_time TIMESTAMP,
    ps_updated_time    TIMESTAMP,
    ps_dw_updated_time TIMESTAMP,
)


create table alefdw.dim_pathway_subject_association
(
    ps_dw_id           bigint,
    ps_pathway_id      varchar(36),
    ps_pathway_dw_id   bigint,
    ps_subject_id      int,
    ps_status          int,
    ps_created_time    TIMESTAMP,
    ps_dw_created_time TIMESTAMP,
    ps_updated_time    TIMESTAMP,
    ps_dw_updated_time TIMESTAMP,
)
foreign key(ps_pathway_dw_id) references alefdw_stage.rel_dw_id_mappings(dw_id)

alter table alefdw_stage.rel_course_content_repository rename to rel_course_content_repository_association;
alter table alefdw.dim_course_content_repository rename to dim_course_content_repository_association;
alter table alefdw_stage.rel_course_curriculum rename to rel_course_curriculum_association;
alter table alefdw.dim_course_curriculum rename to dim_course_curriculum_association;



CREATE TABLE alefdw_stage.rel_tag
(
    tag_dw_id BIGINT,
    tag_created_time TIMESTAMP,
    tag_updated_time TIMESTAMP,
    tag_dw_created_time TIMESTAMP,
    tag_dw_updated_time TIMESTAMP,
    tag_id VARCHAR(36),
    tag_name VARCHAR(1024),
    tag_status INT,
    tag_type VARCHAR(36),
    tag_association_id VARCHAR(36),
    tag_association_type INT,
    tag_association_attach_status INT
);

create table alefdw.dim_course_ability_test_association (
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
)


CREATE TABLE alefdw_stage.rel_tag
(
    tag_dw_id BIGINT,
    tag_created_time TIMESTAMP,
    tag_updated_time TIMESTAMP,
    tag_dw_created_time TIMESTAMP,
    tag_dw_updated_time TIMESTAMP,
    tag_id VARCHAR(36),
    tag_name VARCHAR(1024),
    tag_status INT,
    tag_type VARCHAR(36),
    tag_association_id VARCHAR(36),
    tag_association_type INT,
    tag_association_attach_status INT
);

CREATE TABLE alefdw_stage.rel_course_activity_association
(
    caa_dw_id                      BIGINT,
    caa_created_time               TIMESTAMP,
    caa_updated_time               TIMESTAMP,
    caa_deleted_time               TIMESTAMP,
    caa_dw_created_time            TIMESTAMP,
    caa_dw_updated_time            TIMESTAMP,
    caa_dw_deleted_time            TIMESTAMP,
    caa_status                     INT,
    caa_attach_status              INT,
    caa_course_id                  VARCHAR(36),
    caa_container_id               VARCHAR(36),
    caa_activity_id                VARCHAR(36),
    caa_activity_type              Int,
    caa_activity_pacing            VARCHAR(50),
    caa_activity_index             Int,
    caa_course_version             VARCHAR(10),
    caa_is_parent_deleted          BOOLEAN,
    caa_grade                      VARCHAR(10),
    caa_activity_is_optional       BOOLEAN,
    caa_is_joint_parent_activity   BOOLEAN
);

CREATE TABLE alefdw.dim_course_activity_association
(
    caa_dw_id                      BIGINT,
    caa_created_time               TIMESTAMP,
    caa_updated_time               TIMESTAMP,
    caa_deleted_time               TIMESTAMP,
    caa_dw_created_time            TIMESTAMP,
    caa_dw_updated_time            TIMESTAMP,
    caa_dw_deleted_time            TIMESTAMP,
    caa_status                     INT,
    caa_attach_status              INT,
    caa_course_id                  VARCHAR(36),
    caa_course_dw_id               BIGINT,
    caa_container_id               VARCHAR(36),
    caa_container_dw_id            BIGINT,
    caa_activity_id                VARCHAR(36),
    caa_activity_dw_id             BIGINT,
    caa_activity_type              Int,
    caa_activity_pacing            VARCHAR(50),
    caa_activity_index             Int,
    caa_course_version             VARCHAR(10),
    caa_is_parent_deleted          BOOLEAN,
    caa_grade                      VARCHAR(10),
    caa_activity_is_optional       BOOLEAN,
    caa_is_joint_parent_activity   BOOLEAN
)
foreign key(caa_course_dw_id) references alefdw_stage.rel_dw_id_mappings(dw_id)
foreign key(caa_container_dw_id) references alefdw_stage.rel_dw_id_mappings(dw_id)


create table alefdw_stage.rel_course_activity_container_domain
(
    cacd_dw_id BIGINT,
    cacd_container_id VARCHAR(36),
    cacd_course_id VARCHAR(36),
    cacd_domain VARCHAR(50),
    cacd_sequence INT,
    cacd_created_time TIMESTAMP,
    cacd_dw_created_time TIMESTAMP,
    cacd_updated_time TIMESTAMP,
    cacd_dw_updated_time TIMESTAMP,
    cacd_status INT
);

create table alefdw.dim_course_activity_container_domain
(
    cacd_dw_id BIGINT,
    cacd_container_id VARCHAR(36),
    cacd_container_dw_id BIGINT,
    cacd_course_id VARCHAR(36),
    cacd_course_dw_id BIGINT,
    cacd_domain VARCHAR(50),
    cacd_sequence INT,
    cacd_created_time TIMESTAMP,
    cacd_dw_created_time TIMESTAMP,
    cacd_updated_time TIMESTAMP,
    cacd_dw_updated_time TIMESTAMP,
    cacd_status INT
);

create table alefdw_stage.rel_academic_calendar (
                                                        academic_calendar_dw_id bigint,
                                                        academic_calendar_created_time timestamp ,
                                                        academic_calendar_updated_time timestamp ,
                                                        academic_calendar_deleted_time timestamp ,
                                                        academic_calendar_dw_created_time timestamp ,
                                                        academic_calendar_dw_updated_time timestamp ,
                                                        academic_calendar_status int ,
                                                        academic_calendar_tenant_id varchar(36),
                                                        academic_calendar_title varchar(50),
                                                        academic_calendar_id varchar(36),
                                                        academic_calendar_school_id varchar(36),
                                                        academic_calendar_is_default boolean,
                                                        academic_calendar_type varchar(30),
                                                        academic_calendar_organization varchar(50),
                                                        academic_calendar_academic_year_id varchar(36),
                                                        academic_calendar_created_by_id varchar(36),
                                                        academic_calendar_updated_by_id varchar(36),
                                                        academic_calendar_organization_code varchar(20)
);

create table alefdw.dim_academic_calendar (
                                                  academic_calendar_dw_id bigint,
                                                  academic_calendar_created_time timestamp ,
                                                  academic_calendar_updated_time timestamp ,
                                                  academic_calendar_deleted_time timestamp ,
                                                  academic_calendar_dw_created_time timestamp ,
                                                  academic_calendar_dw_updated_time timestamp ,
                                                  academic_calendar_status int ,
                                                  academic_calendar_title varchar(50),
                                                  academic_calendar_id varchar(36),
                                                  academic_calendar_tenant_id varchar(36),
                                                  academic_calendar_school_id varchar(36),
                                                  academic_calendar_school_dw_id bigint,
                                                  academic_calendar_is_default boolean,
                                                  academic_calendar_type varchar(30),
                                                  academic_calendar_academic_year_id varchar(36),
                                                  academic_calendar_academic_year_dw_id varchar(36),
                                                  academic_calendar_created_by_id varchar(36),
                                                  academic_calendar_updated_by_id varchar(36),
                                                  academic_calendar_organization varchar(50),
                                                  academic_calendar_organization_dw_id bigint,
                                                  academic_calendar_created_by_dw_id bigint,
                                                  academic_calendar_updated_by_dw_is bigint
)

create table alefdw.dim_academic_calendar_teaching_period (
                                                              actp_teaching_period_title varchar(50),
                                                              actp_academic_calendar_id varchar(36),
                                                              actp_dw_updated_time timestamp,
                                                              actp_teaching_period_is_current boolean,
                                                              actp_teaching_period_id varchar(36),
                                                              actp_teaching_period_start_date Date,
                                                              actp_teaching_period_end_date Date,
                                                              actp_dw_created_time timestamp,
                                                              actp_updated_time timestamp,
                                                              actp_created_time timestamp,
                                                              actp_dw_id bigint,
                                                              actp_teaching_period_created_by_id varchar(36),
                                                              actp_teaching_period_updated_by_id varchar(36),
                                                              actp_status int
)

ALTER TABLE alefdw_stage.staging_learning_experience add column fle_source VARCHAR(10) default null;
ALTER TABLE alefdw.fact_learning_experience add column fle_source VARCHAR(10) default null;


CREATE TABLE alefdw_stage.rel_course_activity_outcome_association
(
    caoa_dw_id                      BIGINT,
    caoa_created_time               TIMESTAMP,
    caoa_updated_time               TIMESTAMP,
    caoa_dw_created_time            TIMESTAMP,
    caoa_dw_updated_time            TIMESTAMP,
    caoa_status                     INT,
    caoa_course_id                  VARCHAR(36),
    caoa_activity_id                VARCHAR(36),
    caoa_outcome_id                 VARCHAR(36),
    caoa_outcome_type               VARCHAR(50),
    caoa_curr_id                    BIGINT,
    caoa_curr_grade_id              BIGINT,
    caoa_curr_subject_id            BIGINT
);

CREATE TABLE alefdw.dim_course_activity_outcome_association
(
    caoa_dw_id                      BIGINT,
    caoa_created_time               TIMESTAMP,
    caoa_updated_time               TIMESTAMP,
    caoa_dw_created_time            TIMESTAMP,
    caoa_dw_updated_time            TIMESTAMP,
    caoa_status                     INT,
    caoa_course_dw_id               BIGINT,
    caoa_course_id                  VARCHAR(36),
    caoa_activity_dw_id             BIGINT,
    caoa_activity_id                VARCHAR(36),
    caoa_outcome_id                 VARCHAR(36),
    caoa_outcome_type               VARCHAR(50),
    caoa_curr_id                    BIGINT,
    caoa_curr_grade_id              BIGINT,
    caoa_curr_subject_id            BIGINT
);

CREATE TABLE alefdw.dim_school_academic_year_association
(
    saya_dw_id BIGINT,
    saya_school_id varchar(36),
    saya_academic_year_id varchar(36),
    saya_status int,
    saya_created_time TIMESTAMP,
    saya_updated_time TIMESTAMP,
    saya_dw_created_time TIMESTAMP,
    saya_dw_updated_time TIMESTAMP
)

CREATE TABLE alefdw_stage.rel_course_activity_grade_association
(
    caga_dw_id                      BIGINT,
    caga_created_time               TIMESTAMP,
    caga_updated_time               TIMESTAMP,
    caga_deleted_time               TIMESTAMP,
    caga_dw_created_time            TIMESTAMP,
    caga_dw_updated_time            TIMESTAMP,
    caga_dw_deleted_time            TIMESTAMP,
    caga_status                     INT,
    caga_course_id                  VARCHAR(36),
    caga_activity_id                VARCHAR(36),
    caga_grade_id                   BIGINT
);

CREATE TABLE alefdw.dim_course_activity_grade_association
(
    caga_dw_id                      BIGINT,
    caga_created_time               TIMESTAMP,
    caga_updated_time               TIMESTAMP,
    caga_deleted_time               TIMESTAMP,
    caga_dw_created_time            TIMESTAMP,
    caga_dw_updated_time            TIMESTAMP,
    caga_dw_deleted_time            TIMESTAMP,
    caga_status                     INT,
    caga_course_dw_id               BIGINT,
    caga_course_id                  VARCHAR(36),
    caga_activity_dw_id             BIGINT,
    caga_activity_id                VARCHAR(36),
    caga_grade_id                   BIGINT
);

alter table alefdw.dim_class add column class_academic_calendar_id varchar(36);
alter table alefdw_stage.rel_class add column class_academic_calendar_id varchar(36);

ALTER TABLE alefdw_stage.staging_experience_submitted ADD COLUMN fes_teaching_period_id varchar(36);
ALTER TABLE alefdw_stage.staging_experience_submitted ADD COLUMN fes_academic_year varchar(5);
ALTER TABLE alefdw.fact_experience_submitted ADD COLUMN fes_teaching_period_id varchar(36);
ALTER TABLE alefdw.fact_experience_submitted ADD COLUMN fes_academic_year varchar(5);

ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_teaching_period_id varchar(36);
ALTER TABLE alefdw_stage.staging_learning_experience ADD COLUMN fle_academic_year varchar(5);
ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_teaching_period_id varchar(36);
ALTER TABLE alefdw.fact_learning_experience ADD COLUMN fle_academic_year varchar(5);

ALTER TABLE alefdw_stage.staging_adt_next_question DROP COLUMN fanq_see;

ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_grade int;
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_grade_id varchar(36);
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_academic_year int;
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_academic_year_id varchar(36);
ALTER TABLE alefdw_stage.staging_adt_next_question ADD COLUMN fanq_academic_term smallint;


ALTER TABLE alefdw.fact_adt_next_question DROP COLUMN fanq_see;

ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_grade int;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_grade_id varchar(36);
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_grade_dw_id bigint;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_academic_year int;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_academic_year_id varchar(36);
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_academic_year_dw_id bigint;
ALTER TABLE alefdw.fact_adt_next_question ADD COLUMN fanq_academic_term smallint;

ALTER TABLE alefdw_stage.staging_adt_student_report DROP COLUMN fasr_final_see;
ALTER TABLE alefdw_stage.staging_adt_student_report DROP COLUMN fasr_final_level;

ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_final_category varchar(50);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_grade int;
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_grade_id varchar(36);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_academic_year_id varchar(36);
ALTER TABLE alefdw_stage.staging_adt_student_report ADD COLUMN fasr_secondary_result varchar(50);


ALTER TABLE alefdw.fact_adt_student_report DROP COLUMN fasr_final_see;
ALTER TABLE alefdw.fact_adt_student_report DROP COLUMN fasr_final_level;

ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_final_category varchar(50);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_grade int;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_grade_id varchar(36);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_grade_dw_id bigint;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_academic_year_id varchar(36);
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_academic_year_dw_id bigint;
ALTER TABLE alefdw.fact_adt_student_report ADD COLUMN fasr_secondary_result varchar(50);


ALTER TABLE alefdw.dim_academic_year ADD COLUMN academic_year_type VARCHAR(36);
ALTER TABLE alefdw_stage.rel_academic_year ADD COLUMN academic_year_type VARCHAR(36);
ALTER TABLE alefdw_stage.staging_user_avatar ADD COLUMN fua_avatar_type VARCHAR(36);
ALTER TABLE alefdw_stage.staging_user_avatar ADD COLUMN fua_avatar_file_id VARCHAR(36);
ALTER TABLE alefdw.fact_user_avatar ADD COLUMN fua_avatar_type VARCHAR(36);
ALTER TABLE alefdw.fact_user_avatar ADD COLUMN fua_avatar_file_id VARCHAR(36);

CREATE TABLE alefdw.fact_pathway_teacher_activity (
    fpta_dw_id BIGINT,
    fpta_created_time TIMESTAMP,
    fpta_dw_created_time TIMESTAMP,
    fpta_date_dw_id BIGINT,
    fpta_student_id VARCHAR(36),
    fpta_level_id VARCHAR(36),
    fpta_pathway_id VARCHAR(36),
    fpta_tenant_id VARCHAR(36),
    fpta_action_name VARCHAR(255),
    fpta_class_id VARCHAR(36),
    fpta_teacher_id VARCHAR(36),
    fpta_activity_id VARCHAR(36),
    fpta_action_time TIMESTAMP,
    fpta_tenant_dw_id BIGINT,
    fpta_student_dw_id BIGINT,
    fpta_level_dw_id BIGINT,
    fpta_pathway_dw_id BIGINT,
    fpta_class_dw_id BIGINT,
    fpta_teacher_dw_id BIGINT
);

CREATE TABLE alefdw_stage.staging_pathway_teacher_activity (
   fpta_dw_id BIGINT,
   fpta_created_time TIMESTAMP,
   fpta_dw_created_time TIMESTAMP,
   fpta_date_dw_id BIGINT,
   fpta_student_id VARCHAR(36),
   fpta_level_id VARCHAR(36),
   fpta_pathway_id VARCHAR(36),
   fpta_tenant_id VARCHAR(36),
   fpta_action_name VARCHAR(255),
   fpta_class_id VARCHAR(36),
   fpta_teacher_id VARCHAR(36),
   fpta_activity_id VARCHAR(36),
   fpta_action_time TIMESTAMP
);

alter table alefdw.fact_pathway_teacher_activity add column fpta_activity_dw_id bigint;

alter table alefdw.fact_pathway_teacher_activity add column fpta_activity_type int;
alter table alefdw_stage.staging_pathway_teacher_activity add column fpta_activity_type int;

alter table alefdw.fact_pathway_teacher_activity add column fpta_start_date date;
alter table alefdw.fact_pathway_teacher_activity add column fpta_end_date date;

alter table alefdw_stage.staging_pathway_teacher_activity add column fpta_start_date date;
alter table alefdw_stage.staging_pathway_teacher_activity add column fpta_end_date date;

alter table alefdw.fact_pathway_teacher_activity add column fpta_course_dw_id bigint;
alter table alefdw.fact_pathway_teacher_activity add column fpta_course_activity_container_dw_id bigint;

alter table alefdw_stage.staging_pathway_teacher_activity add column fpta_activity_progress_status varchar(30);
alter table alefdw.fact_pathway_teacher_activity add column fpta_activity_progress_status varchar(30);

create table alefdw.dim_pacing_guide (
                                         pacing_id varchar(36) ,
                                         pacing_dw_id bigint,
                                         pacing_course_id varchar(36),
                                         pacing_course_dw_id bigint,
                                         pacing_class_id varchar(36),
                                         pacing_class_dw_id bigint,
                                         pacing_academic_calendar_id varchar(36),
                                         pacing_academic_year_id varchar(36),
                                         pacing_activity_id varchar(36),
                                         pacing_activity_dw_id bigint,
                                         pacing_tenant_id varchar(36),
                                         pacing_tenant_dw_id bigint,
                                         pacing_status int,
                                         pacing_activity_order int,
                                         pacing_ip_id varchar(36),
                                         pacing_period_start_date DATE,
                                         pacing_period_label varchar(25),
                                         pacing_period_id varchar(36),
                                         pacing_period_end_date DATE,
                                         pacing_interval_id varchar(36),
                                         pacing_interval_start_date DATE ,
                                         pacing_interval_label varchar(25),
                                         pacing_interval_end_date DATE,
                                         pacing_created_time timestamp ,
                                         pacing_dw_created_time timestamp,
                                         pacing_updated_time timestamp ,
                                         pacing_dw_updated_time timestamp
)

create table alefdw_stage.rel_pacing_guide (
                                               pacing_id varchar(36) ,
                                               pacing_dw_id bigint,
                                               pacing_course_id varchar(36),
                                               pacing_class_id varchar(36),
                                               pacing_academic_calendar_id varchar(36),
                                               pacing_academic_year_id varchar(36),
                                               pacing_activity_id varchar(36),
                                               pacing_tenant_id varchar(36),
                                               pacing_status int,
                                               pacing_activity_order int,
                                               pacing_ip_id varchar(36),
                                               pacing_period_start_date DATE,
                                               pacing_period_label varchar(25),
                                               pacing_period_id varchar(36),
                                               pacing_period_end_date DATE,
                                               pacing_interval_id varchar(36),
                                               pacing_interval_start_date DATE ,
                                               pacing_interval_label varchar(25),
                                               pacing_interval_end_date DATE,
                                               pacing_created_time timestamp ,
                                               pacing_dw_created_time timestamp,
                                               pacing_updated_time timestamp ,
                                               pacing_dw_updated_time timestamp
)
alter table alefdw.fact_tutor_session add column fts_learning_session_id varchar(36)
alter table alefdw_stage.staging_tutor_session add column fts_learning_session_id varchar(36)

CREATE TABLE alefdw.fact_activity_setting
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

CREATE TABLE alefdw_stage.staging_activity_setting
(
    fas_activity_id            VARCHAR(36),
    fas_class_id               VARCHAR(36),
    fas_created_time           TIMESTAMP,
    fas_dw_created_time        TIMESTAMP,
    fas_dw_id                  BIGINT,
    fas_grade_id               VARCHAR(36),
    fas_k12_grade              INT,
    fas_open_path_enabled      BOOLEAN,
    fas_school_id              VARCHAR(36),
    fas_class_gen_subject_name VARCHAR(50),
    fas_teacher_id             VARCHAR(36),
    fas_tenant_id              VARCHAR(36)
);

alter table alefdw_stage.staging_lesson_feedback add column lesson_feedback_teaching_period_id varchar(36);
alter table alefdw.fact_lesson_feedback add column lesson_feedback_teaching_period_id varchar(36);
alter table alefdw_stage.staging_lesson_feedback add column lesson_feedback_teaching_period_title varchar(50);
alter table alefdw.fact_lesson_feedback add column lesson_feedback_teaching_period_title varchar(50);

alter table alefdw_stage.rel_assignment_instance add column assignment_instance_teaching_period_id varchar(36);
alter table alefdw.dim_assignment_instance add column assignment_instance_teaching_period_id varchar(36);

alter table alefdw_stage.staging_assignment_submission add column assignment_submission_resubmission_count int default 0;
alter table alefdw.fact_assignment_submission add column assignment_submission_resubmission_count int default 0;

ALTER TABLE alefdw.dim_pathway ADD COLUMN pathway_program_enabled BOOLEAN;
ALTER TABLE alefdw.dim_pathway ADD COLUMN pathway_resources_enabled BOOLEAN;
ALTER TABLE alefdw.dim_pathway ADD COLUMN pathway_placement_type VARCHAR(50);

ALTER TABLE alefdw_stage.rel_pathway ADD COLUMN pathway_program_enabled BOOLEAN;
ALTER TABLE alefdw_stage.rel_pathway ADD COLUMN pathway_resources_enabled BOOLEAN;
ALTER TABLE alefdw_stage.rel_pathway ADD COLUMN pathway_placement_type VARCHAR(50);

ALTER TABLE alefdw.dim_course ADD COLUMN course_program_enabled BOOLEAN;
ALTER TABLE alefdw.dim_course ADD COLUMN course_resources_enabled BOOLEAN;
ALTER TABLE alefdw.dim_course ADD COLUMN course_placement_type VARCHAR(50);

ALTER TABLE alefdw_stage.rel_course ADD COLUMN course_program_enabled BOOLEAN;
ALTER TABLE alefdw_stage.rel_course ADD COLUMN course_resources_enabled BOOLEAN;
ALTER TABLE alefdw_stage.rel_course ADD COLUMN course_placement_type VARCHAR(50);

-- For adding columns practice_material_id and practice_material_type in fact_practice and fact_practice_session tables
ALTER TABLE alefdw_stage.staging_practice ADD COLUMN  practice_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_practice ADD COLUMN  practice_material_type VARCHAR(20);

ALTER TABLE alefdw_stage.staging_practice_session ADD COLUMN  practice_session_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_practice_session ADD COLUMN  practice_session_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_practice ADD COLUMN  practice_material_id VARCHAR(36);

ALTER TABLE alefdw.fact_practice ADD COLUMN  practice_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_practice_session ADD COLUMN  practice_session_material_id VARCHAR(36);

ALTER TABLE alefdw.fact_practice_session ADD COLUMN  practice_session_material_type VARCHAR(20);

-- For adding columns ktg_material_id and ktg_material_type in KTG games tables

ALTER TABLE alefdw_stage.staging_ktg ADD COLUMN  ktg_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_ktg ADD COLUMN  ktg_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_ktg ADD COLUMN  ktg_material_id VARCHAR(36);

ALTER TABLE alefdw.fact_ktg ADD COLUMN  ktg_material_type VARCHAR(20);

ALTER TABLE alefdw_stage.staging_ktgskipped ADD COLUMN  ktgskipped_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_ktgskipped ADD COLUMN  ktgskipped_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_ktgskipped ADD COLUMN  ktgskipped_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_ktgskipped ADD COLUMN  ktgskipped_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_ktg_session ADD COLUMN  ktg_session_material_id VARCHAR(36);

ALTER TABLE alefdw_stage.staging_ktg_session ADD COLUMN  ktg_session_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_ktg_session ADD COLUMN  ktg_session_material_type VARCHAR(20);

ALTER TABLE alefdw.fact_ktg_session ADD COLUMN  ktg_session_material_id VARCHAR(36);

ALTER TABLE alefdw.dim_school_academic_year_association ADD COLUMN saya_previous_academic_year_id varchar(36);
ALTER TABLE alefdw.dim_school_academic_year_association ADD COLUMN saya_type varchar(36);


CREATE TABLE alefdw.dim_course_additional_resource_activity_association (
    caraa_dw_id bigint,
    caraa_course_id varchar(36),
    caraa_resource_activity_id varchar(36),
    caraa_resource_activity_legacy_id varchar(36),
    caraa_resource_activity_type varchar(20),
    caraa_resource_activity_title varchar(50),
    caraa_resource_activity_file_name varchar(50),
    caraa_resource_activity_file_id varchar(50),
    caraa_created_time TIMESTAMP,
    caraa_updated_time TIMESTAMP,
    caraa_dw_created_time TIMESTAMP,
    caraa_dw_updated_time TIMESTAMP,
    caraa_status INT,
    caraa_attach_status INT
);

create table backups.dim_content_repository_date as (select * from alefdw.dim_content_repository);
create table alefdw.dim_content_repository_new (
                                                   content_repository_dw_id bigint,
                                                   content_repository_id varchar(36),
                                                   content_repository_name varchar(50),
                                                   content_repository_organisation_owner varchar(30),
                                                   content_repository_created_by_id varchar(36),
                                                   content_repository_updated_by_id varchar(36),
                                                   content_repository_status int,
                                                   content_repository_created_time timestamp,
                                                   content_repository_dw_created_time timestamp,
                                                   content_repository_updated_time timestamp,
                                                   content_repository_dw_updated_time timestamp
);
--make sure this results into 0 before running insert query to make sure unique dw ids are getting inserted
select content_repository_dw_id from alefdw.dim_content_repository group by content_repository_dw_id having count(1)>1;

insert into alefdw.dim_content_repository_new ( content_repository_dw_id,
                                                content_repository_id ,
                                                content_repository_name ,
                                                content_repository_organisation_owner,
                                                content_repository_status,
                                                content_repository_created_time,
                                                content_repository_dw_created_time,
                                                content_repository_updated_time,
                                                content_repository_dw_updated_time)
select content_repository_dw_id,
       content_repository_id,
       content_repository_name,
       content_repository_organisation_owner,
       content_repository_status,
       content_repository_created_time,
       content_repository_dw_created_time,
       content_repository_updated_time,
       content_repository_dw_updated_time
from alefdw.dim_content_repository;
-- check the count in old and new tables it should be same and dw id are correct
-- update the max dw id in the product max id table

alter table alefdw.dim_content_repository add column content_repository_updated_by_id varchar(36);
alter table alefdw.dim_content_repository add column content_repository_created_by_id varchar(36);

DROP TABLE alefdw_stage.rel_content_repository;
DROP TABLE alefdw.dim_content_repository;
ALTER TABLE alefdw.dim_content_repository_new RENAME TO dim_content_repository;

ALTER TABLE alefdw.dim_role ADD COLUMN role_uuid VARCHAR(36);
ALTER TABLE alefdw.dim_role ADD COLUMN role_created_time TIMESTAMP;
ALTER TABLE alefdw.dim_role ADD COLUMN role_updated_time TIMESTAMP;
ALTER TABLE alefdw.dim_role ADD COLUMN role_dw_created_time TIMESTAMP;
ALTER TABLE alefdw.dim_role ADD COLUMN role_dw_updated_time TIMESTAMP;
ALTER TABLE alefdw.dim_role ADD COLUMN role_organization_name VARCHAR(50);
ALTER TABLE alefdw.dim_role ADD COLUMN role_organization_code VARCHAR(50);
ALTER TABLE alefdw.dim_role ADD COLUMN role_type VARCHAR(50);
ALTER TABLE alefdw.dim_role ADD COLUMN role_category_id VARCHAR(36);
ALTER TABLE alefdw.dim_role ADD COLUMN role_is_ccl BOOLEAN;
ALTER TABLE alefdw.dim_role ADD COLUMN role_status INT;
ALTER TABLE alefdw.dim_role ADD COLUMN role_description VARCHAR(1536);
ALTER TABLE alefdw.dim_role ADD COLUMN role_predefined BOOLEAN;

CREATE TABLE alefdw_stage.rel_adt_attempt_threshold
(
  aat_dw_id              BIGINT,
  aat_created_time       TIMESTAMP,
  aat_updated_time       TIMESTAMP,
  aat_dw_created_time    TIMESTAMP,
  aat_dw_updated_time    TIMESTAMP,
  aat_status             INT,
  aat_id                 VARCHAR(36),
  aat_academic_year_id   VARCHAR(36),
  aat_tenant_id          VARCHAR(36),
  aat_school_id          VARCHAR(36),
  aat_state              VARCHAR(50),
  aat_attempt_title      VARCHAR(100),
  aat_attempt_start_time TIMESTAMP,
  aat_attempt_end_time   TIMESTAMP,
  aat_attempt_number     INT,
  aat_total_attempts     INT
);

CREATE TABLE alefdw.dim_adt_attempt_threshold
(
  aat_dw_id               BIGINT,
  aat_created_time        TIMESTAMP,
  aat_updated_time        TIMESTAMP,
  aat_dw_created_time     TIMESTAMP,
  aat_dw_updated_time     TIMESTAMP,
  aat_status              INT,
  aat_id                  VARCHAR(36),
  aat_tenant_dw_id        BIGINT,
  aat_tenant_id           VARCHAR(36),
  aat_academic_year_dw_id BIGINT,
  aat_academic_year_id    VARCHAR(36),
  aat_school_dw_id        BIGINT,
  aat_school_id           VARCHAR(36),
  aat_state               VARCHAR(50),
  aat_attempt_title       VARCHAR(100),
  aat_attempt_start_time  TIMESTAMP,
  aat_attempt_end_time    TIMESTAMP,
  aat_attempt_number      INT,
  aat_total_attempts      INT
);

alter table alefdw.dim_course_content_repository_association add column ccr_course_type varchar(50);

-- All ddl/dml statements should be above this line
------------------------------------------------------------ * ---------------------------------------------------------

-- Keep Grant commands at the end of the file
----GRANT PERMISSIONS
--For appuser
GRANT USAGE ON SCHEMA alefdw_stage TO GROUP appuser;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw_stage TO GROUP appuser;

GRANT ALL ON SCHEMA alefdw TO GROUP appuser;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw TO GROUP appuser;
REVOKE DELETE ON ALL TABLES IN SCHEMA alefdw FROM GROUP appuser;

--For Devs and QAs --
GRANT USAGE ON SCHEMA alefdw_stage TO GROUP data_engineering;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw_stage TO GROUP data_engineering;
REVOKE DELETE ON ALL TABLES IN SCHEMA alefdw_stage FROM GROUP data_engineering;

GRANT ALL ON SCHEMA alefdw TO GROUP data_engineering;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw TO GROUP data_engineering;
REVOKE DELETE ON ALL TABLES IN SCHEMA alefdw FROM GROUP data_engineering;

-- For TDC
GRANT USAGE ON schema  alefdw TO  GROUP tdc;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP tdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA "alefdw" GRANT SELECT ON TABLES TO GROUP tdc;

-- For Data Science
GRANT USAGE ON schema  alefdw TO  GROUP datascience;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP datascience;
ALTER DEFAULT PRIVILEGES IN SCHEMA "alefdw" GRANT SELECT ON TABLES TO GROUP datascience;

-- For Business Intelligence
GRANT USAGE ON schema  alefdw TO  GROUP business_intelligence;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP business_intelligence;
ALTER DEFAULT PRIVILEGES IN SCHEMA "alefdw" GRANT SELECT ON TABLES TO  GROUP business_intelligence;

-- For ro_user
GRANT USAGE ON schema  alefdw TO  GROUP ro_users;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA "alefdw" GRANT SELECT ON TABLES TO  GROUP ro_users;

GRANT USAGE ON schema  alefdw_stage TO  GROUP ro_users;
GRANT SELECT ON ALL TABLES IN  schema alefdw_stage  TO  GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA "alefdw_stage" GRANT SELECT ON TABLES TO  GROUP ro_users;

-- For adding columns practice_material_id and practice_material_type in fact_practice and fact_practice_session tables