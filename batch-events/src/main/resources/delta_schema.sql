CREATE TABLE IF NOT EXISTS alefdatalake.dim_assignment_instance (
  assignment_instance_id string,
  assignment_instance_assignment_id string,
  assignment_instance_created_time timestamp,
  assignment_instance_dw_created_time timestamp,
  assignment_instance_updated_time timestamp,
  assignment_instance_deleted_time timestamp,
  assignment_instance_dw_updated_time timestamp,
  assignment_instance_status int,
  assignment_instance_teacher_id string,
  assignment_instance_grade_id string,
  assignment_instance_subject_id string,
  assignment_instance_class_id string,
  assignment_instance_learning_path_id string,
  assignment_instance_lo_id string,
  assignment_instance_student_id string,
  assignment_instance_tenant_id string,
  assignment_instance_due_on string,
  assignment_instance_allow_late_submission boolean,
  assignment_instance_type string,
  assignment_instance_start_on long,
  assignment_instance_student_status boolean,
  eventdate Date)
  USING DELTA
  COMMENT 'This table is created with no data'
  PARTITIONED BY (eventdate)
  LOCATION 's3a://alef-bigdata-ireland/lakehouse/production/alef-assignment-instance/';

create table if not exists alefdatalake.fact_assignment_submission(
	assignment_submission_id string,
	assignment_submission_assignment_instance_id string,
	assignment_submission_date_dw_id integer,
	assignment_submission_created_time timestamp,
	assignment_submission_dw_created_time timestamp,
	assignment_submission_status string,
	assignment_submission_student_id string,
	assignment_submission_teacher_id string,
	assignment_submission_tenant_id string,
	assignment_submission_student_attachment_file_name string,
	assignment_submission_student_attachment_path string,
	assignment_submission_teacher_attachment_file_name string,
	assignment_submission_teacher_attachment_path string,
	assignment_submission_teacher_comment string,
	assignment_submission_teacher_score double,
	assignment_submission_student_comment string,
  assignment_submission_updated_on Long,
  assignment_submission_returned_on Long,
  assignment_submission_submitted_on Long,
  assignment_submission_graded_on Long,
	eventdate Date)
  USING DELTA
  COMMENT 'This table is created with no data'
  PARTITIONED BY (eventdate)
  LOCATION 's3a://alef-bigdata-ireland/lakehouse/production/alef-assignment-submission/';

CREATE TABLE IF NOT EXISTS alefdatalake.dim_assignment(
	assignment_id string,
	assignment_title string,
	assignment_description string,
	assignment_max_score double,
	assignment_attachment_file_name string,
	assignment_attachment_path string,
	assignment_is_gradeable boolean,
	assignment_allow_submission boolean,
	assignment_school_id string,
	assignment_language string,
	assignment_status string,
	assignment_created_by string,
	assignment_updated_by string,
	assignment_created_on Long,
    assignment_updated_on Long,
    assignment_published_on Long,

	assignment_created_time timestamp,
    assignment_dw_created_time timestamp,
    assignment_updated_time timestamp,
    assignment_deleted_time timestamp,
    assignment_dw_updated_time timestamp,
	eventdate Date)
  USING DELTA
  PARTITIONED BY (eventdate)
  LOCATION 's3a://alef-bigdata-ireland/lakehouse/production/alef-assignment/';


create table if not exists alefdw.dim_subject
(
	subject_status integer,
	subject_id string,
	subject_name string,
	subject_online boolean,
	subject_gen_subject string,
	grade_id string,

	subject_created_time timestamp,
    subject_updated_time timestamp,
    subject_deleted_time timestamp,
    subject_dw_created_time timestamp,
    subject_dw_updated_time timestamp,
    eventdate Date
)
USING DELTA
  PARTITIONED BY (eventdate)
  LOCATION 's3a://alef-bigdata-ireland/lakehouse/production/alef-subject/';


alter table alefdatalakeproduction.dim_class_user add column class_user_attach_status INT;


alter table alefdatalakeproduction.dim_skill_association add column skill_association_is_previous BOOLEAN;


alter table alefdatalakeproduction.dim_pathway_level_activity_association add column plaa_pathway_id string;

CREATE TABLE alefdatalakeproduction.dim_badge
(
    bdg_id STRING
    ,bdg_tier STRING
    ,bdg_grade INTEGER
    ,bdg_type STRING
    ,bdg_tenant_id STRING
    ,bdg_title STRING
    ,bdg_category STRING
    ,bdg_threshold INTEGER
    ,bdg_status INTEGER
    ,bdg_created_time TIMESTAMP
    ,bdg_dw_created_time TIMESTAMP
    ,bdg_active_until TIMESTAMP
)
    USING delta
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-badge';




CREATE OR REPLACE TABLE alefdatalakeproduction.fact_badge_awarded
(
    ,fba_id	String
    ,fba_created_time TIMESTAMP
    ,fba_badge_id	String
    ,fba_badge_tier String
    ,fba_badge_type String
    ,fba_grade_id String
    ,fba_student_id	String
    ,fba_school_id String
    ,fba_section_id String
    ,fba_tenant_id String
    ,fba_academic_year_id	String
    ,fba_date_dw_id bigint
    ,fba_dw_created_time TIMESTAMP
    ,eventdate String
)
    USING delta
PARTITIONED BY (eventdate)
LOCATION 's3://alef-bigdata-emr/lakehouse/production/alef-badge-awarded'
TBLPROPERTIES (
	'delta.autoOptimize.autoCompact' = 'true',
	'delta.autoOptimize.optimizeWrite' = 'true',
	'delta.minReaderVersion' = '1',
	'delta.minWriterVersion' = '2'
);


-- Refer this notebook - https://alef-bigdata-prod.cloud.databricks.com/?o=305700452284325#notebook/2007286390270174/command/3799630298820307
CREATE OR REPLACE TABLE alefdatalakeproduction.fact_student_certificate_awarded_timur
(
    fsca_created_time TIMESTAMP
    ,fsca_dw_created_time TIMESTAMP
    ,fsca_date_dw_id bigint
    ,fsca_id	String
    ,fsca_student_id	String
    ,fsca_award_category	String
    ,fsca_academic_year_id	String
    ,fsca_class_id	String
    ,fsca_grade_id	String
    ,fsca_teacher_id	String
    ,fsca_language	String
    ,fsca_tenant_id	String
    ,eventdate String
)
    USING delta
PARTITIONED BY (eventdate)
LOCATION 's3://alef-bigdata-emr/lakehouse/production/alef-student-certificate-awarded-timur'
TBLPROPERTIES (
	'delta.autoOptimize.autoCompact' = 'true',
	'delta.autoOptimize.optimizeWrite' = 'true',
	'delta.minReaderVersion' = '1',
	'delta.minWriterVersion' = '2'
);

CREATE OR REPLACE TABLE alefdatalakeproduction.fact_jira_issue
(
    fji_created_time TIMESTAMP
    ,fji_dw_created_time TIMESTAMP
    ,fji_due_date TIMESTAMP
    ,fji_date_dw_id bigint
    ,fji_key	String
    ,fji_summary	String
    ,fji_labels	String
    ,fji_percent_of_spent_time	INT
    ,fji_original_estimate	BIGINT
    ,fji_status	String
    ,fji_priority	String
    ,fji_reporter	String
    ,fji_assignee	String
    ,fji_issue_type	String
    ,fji_resolution	String
    ,eventdate String
)
    USING delta
PARTITIONED BY (eventdate)
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-jira-issue'
TBLPROPERTIES (
	'delta.autoOptimize.autoCompact' = 'true',
	'delta.autoOptimize.optimizeWrite' = 'true',
	'delta.minReaderVersion' = '1',
	'delta.minWriterVersion' = '2'
)


CREATE TABLE hive_metastore.alefdatalakeproduction.fact_guardian_joint_activity
(
    fgja_guardian_id string,
    fgja_school_id string,
    fgja_pathway_id string,
    fgja_pathway_level_id string,
    fgja_k12_grade integer,
    fgja_rating integer,
    fgja_attempt integer,
    fgja_tenant_id string,
    fgja_state integer,
    fgja_class_id string,
    fgja_student_id string,
    eventdate string,
    fgja_date_dw_id string,
    fgja_created_time timestamp,
    fgja_dw_created_time timestamp
)
    USING delta
PARTITIONED BY (eventdate)
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-guardian-joint-activity/';


ALTER TABLE alefdatalakeproduction.dim_content_repository RENAME COLUMN content_repository_organization TO content_repository_organisation_owner;


-- activity setting schema --
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType}
import org.apache.spark.sql.Row
val schema = new StructType()
    .add(StructField("fas_activity_dw_id", LongType))
    .add(StructField("fas_activity_id", StringType))
    .add(StructField("fas_class_dw_id", LongType))
    .add(StructField("fas_class_id", StringType))
    .add(StructField("fas_created_time", TimestampType))
    .add(StructField("fas_dw_created_time", TimestampType))
    .add(StructField("fas_dw_id", LongType))
    .add(StructField("fas_grade_dw_id", LongType))
    .add(StructField("fas_grade_id", StringType))
    .add(StructField("fas_k12_grade", IntegerType ))
    .add(StructField("fas_open_path_enabled", BooleanType))
    .add(StructField("fas_school_dw_id", LongType))
    .add(StructField("fas_school_id", StringType))
    .add(StructField("fas_class_gen_subject_name", StringType))
    .add(StructField("fas_teacher_dw_id", LongType))
    .add(StructField("fas_teacher_id", StringType))
    .add(StructField("fas_tenant_dw_id", LongType))
    .add(StructField("fas_tenant_id", StringType))

val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
df.write.format("delta").mode("overwrite").save("s3://alef-bigdata-ireland/lakehouse/production/alef-activity-settings-open-path/")

-- pacing guide schema ---
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, LongType, DateType, DoubleType, BooleanType}
import org.apache.spark.sql.Row
val schema = new StructType()
.add(StructField("pacing_id", StringType))
.add(StructField("pacing_dw_id", LongType))
.add(StructField("pacing_course_id", StringType))
.add(StructField("pacing_course_dw_id", LongType))
.add(StructField("pacing_class_id", StringType))
.add(StructField("pacing_class_dw_id", LongType))
.add(StructField("pacing_academic_calendar_id", StringType))
.add(StructField("pacing_academic_year_id", StringType))
.add(StructField("pacing_activity_id", StringType))
.add(StructField("pacing_activity_dw_id", LongType))
.add(StructField("pacing_tenant_id", StringType))
.add(StructField("pacing_tenant_dw_id", LongType))
.add(StructField("pacing_status", IntegerType ))
.add(StructField("pacing_activity_order", IntegerType))
.add(StructField("pacing_ip_id", StringType))
.add(StructField("pacing_period_start_date", DateType))
.add(StructField("pacing_period_label", StringType))
.add(StructField("pacing_period_id", StringType))
.add(StructField("pacing_period_end_date", DateType))
.add(StructField("pacing_interval_id", StringType))
.add(StructField("pacing_interval_start_date", DateType))
.add(StructField("pacing_interval_label", StringType))
.add(StructField("pacing_interval_end_date", DateType))
.add(StructField("pacing_created_time", TimestampType))
.add(StructField("pacing_dw_created_time", TimestampType))
.add(StructField("pacing_updated_time", TimestampType))
.add(StructField("pacing_dw_updated_time", TimestampType))
val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
df.write.format("delta").mode("overwrite").save("s3://alef-bigdata-ireland/lakehouse/production/alef-pacing-guide/")
----- end -----------

UPDATE hive_metastore.alefdatalakeproduction.fact_learning_experience
SET lo_uuid = lo_id,
    student_uuid = student_id,
    subject_uuid = subject_id,
    section_uuid = section_id,
    class_uuid = class_id,
    curr_uuid = curr_id,
    curr_subject_uuid = curr_subject_id,
    curr_grade_uuid = curr_grade_id,
    academic_year_uuid = academic_year_id,
    school_uuid = school_id,
    lp_uuid = lp_id,
    tenant_uuid = tenant_id
WHERE fle_state = 3 and fle_material_type != 'PATHWAY'
and academic_year_id is not null
and fle_date_dw_id between 20230101 and 20240613;


CREATE TABLE `alefdatalakeproduction`.`product_max_ids` (
`table_name` STRING,
`max_id` BIGINT,
`status` STRING,
`updated_time` TIMESTAMP)
    USING delta
PARTITIONED BY (table_name)
LOCATION 's3://alef-bigdata-ireland/lakehouse/internal/product-max-ids/'

CREATE TABLE `alefdatalakeproduction`.`dim_avatar`
(
  avatar_dw_id BIGINT NOT NULL,
  avatar_id STRING NOT NULL,
  avatar_file_id STRING NOT NULL,
  avatar_created_time TIMESTAMP,
  avatar_deleted_time TIMESTAMP,
  avatar_dw_created_time TIMESTAMP,
  avatar_updated_time TIMESTAMP,
  avatar_dw_updated_time TIMESTAMP,
  avatar_app_status STRING,
  avatar_status INTEGER,
  avatar_type STRING,
  avatar_name STRING,
  avatar_description STRING,
  avatar_valid_from TIMESTAMP,
  avatar_valid_till TIMESTAMP,
  avatar_category STRING,
  avatar_star_cost INTEGER,
  avatar_genders ARRAY<STRING>,
  avatar_created_by STRING,
  avatar_created_at STRING,
  avatar_updated_by STRING,
  avatar_updated_at STRING,
  avatar_tenants ARRAY<STRING>,
  avatar_organizations ARRAY<STRING>
)
USING delta
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-marketplace-avatar/'
;

-- core activity assign schema --
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType}
import org.apache.spark.sql.Row
val schema = new StructType()
.add(StructField("cta_dw_id", LongType))
.add(StructField("cta_event_type", StringType))
.add(StructField("cta_id", StringType))
.add(StructField("cta_created_time", TimestampType))
.add(StructField("cta_dw_created_time", TimestampType))
.add(StructField("cta_status", IntegerType))
.add(StructField("cta_active_until", TimestampType))
.add(StructField("cta_action_time", TimestampType))
.add(StructField("cta_ay_tag", StringType ))
.add(StructField("cta_tenant_id", StringType ))
.add(StructField("cta_student_id", StringType ))
.add(StructField("cta_course_id", StringType ))
.add(StructField("cta_class_id", StringType ))
.add(StructField("cta_teacher_id", StringType ))
.add(StructField("cta_activity_id", StringType))
.add(StructField("cta_activity_type", StringType))
.add(StructField("cta_progress_status", StringType))
.add(StructField("cta_start_date", DateType))
.add(StructField("cta_end_date", DateType))

val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
df.write.format("delta").mode("overwrite").save("s3://alef-bigdata-ireland/lakehouse/production/alef-core-activity-assigned/")

--ALEF-70775 ADD column to identify if test is used for placement
UPDATE alefdatalakeproduction.dim_course_ability_test_association SET cata_ability_test_activity_type = 'TestActivity' WHERE cata_ability_test_activity_type IS NULL;
UPDATE alefdatalakeproduction.dim_course_ability_test_association SET cata_is_placement_test = true WHERE cata_is_placement_test IS NULL;


CREATE TABLE IF NOT EXISTS alefdatalakeproduction.fact_assessment_lock_action (
  dw_id BIGINT,
  _trace_id String,
  event_type String,
  created_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  date_dw_id BIGINT,
  id String,
  tenant_id String,
  academic_year_tag String,
  attempt BIGINT,
  teacher_id String,
  candidate_id String,
  school_id String,
  class_id String,
  test_part_session_id String,
  test_level_name String,
  test_level_id String,
  test_level_version BIGINT,
  skill String,
  subject String,
  eventdate String
)
USING delta
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-candidate-assessment-lock-unlock/';


CREATE TABLE IF NOT EXISTS alefdatalakeproduction.fact_candidate_assessment_progress(
  dw_id BIGINT,
  _trace_id String,
  event_type String,
  created_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  date_dw_id BIGINT,
  tenant_id String,
  academic_year_tag String,
  assessment_id String,
  school_id String,
  grade_id String,
  candidate_id String,
  grade INT,
  material_type String,
  attempt_number INT,
  skill String,
  subject String,
  language String,
  status String,
  test_level_session_id String,
  test_level String,
  test_id String,
  test_version BIGINT,
  test_level_id String,
  test_level_version BIGINT,
  test_level_section_id String,
  report_id String,
  total_timespent BIGINT,
  final_score BIGINT,
  final_grade BIGINT,
  final_category String,
  final_uncertainty FLOAT,
  time_to_return BIGINT,
  framework String,
  domains String,
  eventdate String
)
USING delta
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-candidate-assessment-progress/';


CREATE TABLE IF NOT EXISTS alefdatalakeproduction.fact_assessment_answer_submitted (
  dw_id BIGINT,
  _trace_id String,
  event_type String,
  created_time TIMESTAMP,
  dw_created_time TIMESTAMP,
  date_dw_id BIGINT,
  assessment_id String,
  tenant_id String,
  academic_year_tag String,
  attempt_number INT,
  candidate_id String,
  class_id String,
  grade INT,
  grade_id String,
  time_spent INT,
  subject String,
  school_id String,
  language String,
  question_id String,
  question_code String,
  question_version INT,
  test_level_session_id String,
  test_level_version BIGINT,
  test_level_section_id String,
  reference_code String,
  test_level String,
  skill String,
  material_type String,
  answer String,
  eventdate String
)
USING delta
LOCATION 's3://alef-bigdata-ireland/lakehouse/production/alef-assessment-answer-submitted/';

ALTER TABLE alefdatalakeproduction.fact_student_activities DROP COLUMN fsta_ip_address, VACUUM fact_student_activities RETAIN 0 HOURS;
ALTER TABLE alefdatalakeproduction.fact_teacher_activities DROP COLUMN fta_ip_address, VACUUM fact_teacher_activities RETAIN 0 HOURS;

ALTER TABLE alefdatalakeproduction.dim_organization DROP COLUMN organization_email, VACUUM dim_organization RETAIN 0 HOURS;
