------------------------------------ MIGRATION OF ORGANISATION ID -----------------------------------------------
-------------- ON SCHOOL TABLE -------------------
--Deltalake
MERGE INTO alefdatalakeproduction.dim_school s
USING alefdatalakeproduction.dim_organisation o
ON s.school_organisation = o.organisation_name
WHEN MATCHED
  THEN UPDATE set school_organisation_id = o.organisation_id;

------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------------------------------ * ---------------------------------------------------------

--******************************************TO BE RELEASED**************************************************************

------------------------------------ BACKFILLING OF ORGANISATION COLUMN ------------------------------------------------
-------------- ON CONTENT TABLE -------------------
--DELTALAKE--
UPDATE alefdatalakeproduction.dim_content
SET content_organisation = 'shared'
WHERE 1=1;

-------------- ON CURRICULUM TABLE -------------------
--DELTALAKE--
UPDATE alefdatalakeproduction.dim_curriculum
SET curr_organisation = 'shared'
WHERE 1=1;

-------------- ON LEARNING_OBJECTIVE TABLE -------------------
--DELTALAKE--
UPDATE alefdatalakeproduction.dim_learning_objective
SET lo_organisation = 'shared'
WHERE 1=1;

-------------- ON LESSON_TEMPLATE TABLE -------------------
--DELTALAKE--
UPDATE alefdatalakeproduction.dim_template
SET template_organisation = 'shared'
WHERE 1=1;
------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------ BACKFILLING OF FLEXIBLE LESSON COLUMNS --------------------------------------------
--DELTA--
-------------- ON fact_learning_experience TABLE -------------------
MERGE INTO alefdatalakeproduction.fact_learning_experience fle
USING alefdatalakeproduction.dim_learning_objective lo
ON fle.lo_id = lo.lo_id
WHEN MATCHED
  THEN UPDATE set
  fle_activity_template_id    = lo_type;

update alefdatalakeproduction.fact_learning_experience
set fle_activity_type           = fle.fle_lesson_category,
    fle_abbreviation            = fle.fle_lesson_type,
    fle_activity_component_type = case
                                    when fle.fle_lesson_type like 'TEQ%' or
                                    fle.fle_lesson_type = 'SA' or
                                    fle.fle_lesson_type = 'ASGN' then 'ASSIGNMENT'
                                    else case
                                           when fle.fle_lesson_type like 'KT' then 'KEY_TERM'
                                           else 'CONTENT'
            END
        END,
    fle_exit_ticket             = case when fle.fle_lesson_type = 'SA' then true else false END,
    fle_completion_node         = case
                                    when fle.fle_lesson_type = 'MP' or
                                         fle.fle_lesson_type = 'R' or
                                         fle.fle_lesson_type = 'R_2' or
                                         fle.fle_lesson_type = 'R_3'
                                            then false
                                    ELSE true END;

-------------- ON fact_learning_experience TABLE -------------------
update alefdatalakeproduction.fact_experience_submitted
set fes_activity_type           = fes_lesson_category,
    fes_abbreviation            = fes_lesson_type,
    fes_activity_component_type = case
                                    when fes_lesson_type like 'TEQ%' or fes_lesson_type = 'SA' or fes_lesson_type =
                                         'ASGN' then 'ASSIGNMENT'
                                    else case
                                           when fes_lesson_type like 'KT' then 'KEY_TERM'
                                           else 'CONTENT'
            END
        END,
    fes_exit_ticket             = case when fes_lesson_type = 'SA' then true else false END,
    fes_completion_node         = case
                                    when fes_lesson_type = 'MP' or
                                         fes_lesson_type = 'R' or
                                         fes_lesson_type = 'R_2' or
                                         fes_lesson_type = 'R_3'
                                            then false
                                    ELSE true END;
------------------------------------------------------------ * ---------------------------------------------------------

------------------------------------ BACKFILLING OF ASSIGNMENT INSTANCE STUDENT ASSOCIATION TABLE --------------------------------------------
-- QUERY TO MIGRATE STUDENT ASSOCIATION FROM dim_assignment TO assignment_instance_student --
INSERT INTO alefdatalakeproduction.dim_assignment_instance_student (
	ais_created_time,
	ais_dw_created_time,
	ais_updated_time,
	ais_dw_updated_time,
	ais_deleted_time,
	ais_status,
	ais_instance_id,
	ais_student_id
)
SELECT
	    assignment_instance_created_time 		AS 	ais_created_time,
    	assignment_instance_dw_created_time 	AS 	ais_dw_created_time,
    	assignment_instance_updated_time 		AS	ais_updated_time,
    	assignment_instance_dw_updated_time 	AS 	ais_dw_updated_time,
    	assignment_instance_deleted_time 		AS 	ais_deleted_time,
    	assignment_instance_status 				AS 	ais_status,
    	assignment_instance_id 					AS  ais_instance_id,
    	assignment_instance_student_id 			AS 	ais_student_id
FROM alefdatalakeproduction.dim_assignment_instance;

-- DELETION OF OLD RECORDS WILL BE DONE FROM SPARK CODE
------------------------------------------------------------ * ---------------------------------------------------------
-------------------------------- BACKFILLING OF INSTRUCTIONAL PLAN NEW COLUMNS -----------------------------------------
UPDATE alefdatalakeproduction.dim_instructional_plan
SET instructional_plan_item_instructor_led=true,
SET instructional_plan_item_default_locked=true
WHERE 1=1;
------------------------------------------------------------ * ---------------------------------------------------------
-------------------------------- BACKFILLING OF ACADEMIC YEAR is_roll_over_completed COLUMN ----------------------------
UPDATE alefdatalakeproduction.dim_academic_year
SET academic_year_is_roll_over_completed = false
WHERE 1=1;

MERGE INTO
    alefdatalakeproduction.dim_academic_year ay
USING
    (
        SELECT ay2.*,
            ROW_NUMBER() OVER (PARTITION BY academic_year_school_id ORDER BY academic_year_end_date DESC) AS rn
        FROM alefdatalakeproduction.dim_academic_year AS ay2
    ) ranked_ay
ON
    ranked_ay.rn <> 1
    AND ranked_ay.academic_year_id = ay.academic_year_id
    AND ranked_ay.academic_year_school_id = ay.academic_year_school_id
WHEN MATCHED
  THEN
    UPDATE set
        academic_year_is_roll_over_completed = true;
------------------------------------------------------------ * ---------------------------------------------------------
------------------------------------------ ADD NEW COLUMN FOR DIM_CLASS --------------------------------------------------------------
ALTER TABLE alefdatalakeproduction.dim_class ADD COLUMN class_active_until TIMESTAMP;


update alefdatalakeproduction.dim_class_user set class_user_attach_status = class_user_status;
update alefdatalakeproduction.dim_class_user set class_user_status = 2 where class_user_active_until is not null;
update alefdatalakeproduction.dim_class_user set class_user_status = 1 where class_user_active_until is null;



update alefdatalakeproduction.dim_skill_association set skill_association_is_previous = false;

-- BACKFILL MATERIAL ID AND MATERIAL TYPE IN CLASS DIM -----------------------------------------------------------------
-- Redshift
update alefdatalakeproduction.dim_class
set class_material_id = class_curriculum_instructional_plan_id,
class_material_type = 'INSTRUCTIONAL_PLAN';

-- UPDATE step_instance_title to Exit Ticket for template ed0feb97-69f5-40d7-a298-e40a22791cd1 -------------------------
update alefdatalakeproduction.dim_step_instance set step_instance_title = 'Exit Ticket'
where step_instance_template_uuid = 'ed0feb97-69f5-40d7-a298-e40a22791cd1'
and step_instance_title = 'Level Up!';




update alefdatalakeproduction.dim_pathway_level_activity_association set plaa_pathway_id = t.pathway_level_pathway_id
from alefdatalakeproduction.dim_pathway_level_activity_association plaa inner join
     (
         select pathway_level_id, pathway_level_pathway_id from alefdatalakeproduction.dim_pathway_level group by pathway_level_id, pathway_level_pathway_id
     ) t on t.pathway_level_id = plaa.plaa_level_id
where plaa.plaa_pathway_id is null;

-- DELETE DUPLICATES AFTER MOVING flattened CONTENT_REPOSITORY COLUMN TO dim_pathway_content_repository -------------------------
delete
FROM alefdatalakeproduction.dim_pathway
where concat(pathway_dw_id,
  pathway_id,
  pathway_status,
  pathway_name,
  pathway_code,
  pathway_subject_id,
  pathway_organization_dw_id,
  pathway_created_time,
  pathway_dw_created_time,
  pathway_updated_time,
  pathway_lang_code) in (SELECT a.id
FROM (SELECT concat(pathway_dw_id,
             pathway_id,
             pathway_status,
             pathway_name,
             pathway_code,
             pathway_subject_id,
             pathway_organization_dw_id,
             pathway_created_time,
             pathway_dw_created_time,
             pathway_updated_time,
             pathway_lang_code) AS id,
             row_number() over (partition by pathway_dw_id,
             pathway_id,
             pathway_status,
             pathway_name,
             pathway_code,
             pathway_subject_id,
             pathway_organization_dw_id,
             pathway_created_time,
             pathway_updated_time,
             pathway_lang_code
             order by
             pathway_dw_created_time) as rnk
FROM alefdatalakeproduction.dim_pathway) a
WHERE a.rnk > 1);



-- backfill ftc_session_state
update alefdatalakeproduction.fact_tutor_conversation set ftc_session_state = ftc.fts_session_state
from alefdatalakeproduction.fact_tutor_conversation ftc inner join
     (select fts_session_id, max(fts_session_state) as fts_session_state from alefdatalakeproduction.fact_tutor_session group by fts_session_id) fts on
             ftc.ftc_session_id = fts.fts_session_id;


-- migrate fact_service desk table with new dw_id approach in delta notebook
val df = spark.read
.format("com.databricks.spark.redshift")
.option("url", getProdRedshiftUrl)
.option("query", "select * from alefdw.fact_service_desk_request")
.option("tempdir", "s3a://alef-bigdata-ireland/temp_redshift")
.option("forward_spark_s3_credentials", "true")
.option("inferSchema", true)
.option("autoenablessl", "false")
.load()

df.coalesce(1).write
.format("org.apache.spark.sql.parquet")
.option("overwriteSchema", true)
.partitionBy("eventdate")
.mode("overwrite")
.save(s"s3://alef-bigdata-ireland/lakehouse/production/alef-service-desk-request")


-- backfill alefdatalakeproduction.dim_course_activity_container_grade_association
insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('dim_course_activity_container_grade_association', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(cacga_dw_id) from redshift backfill
-- backfill alefdatalakeproduction.dim_course_activity_container_grade_association
insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('dim_course_activity_container_grade_association', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(cacga_dw_id) from redshift backfill

import com.alefeducation.utils.AWSSecretManagerUtils._

val df = spark.read
.format("com.databricks.spark.redshift")
.option("url", getProdRedshiftUrl)
.option("query", "select * from alefdw_stage.rel_course_activity_container_grade_association")
.option("tempdir", "s3a://alef-bigdata-ireland/temp_redshift")
.option("forward_spark_s3_credentials", "true")
.option("inferSchema", true)
.option("autoenablessl", "false")
.load()
.orderBy("cacga_dw_id")

val path = "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-container-grade-association"
df.coalesce(1).write.format("delta").mode("append").save(path)

-- backfill alefdatalakeproduction.dim_course_activity_container_domain
insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('dim_course_activity_container_domain', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(cacd_dw_id) from redshift backfill


import com.alefeducation.utils.AWSSecretManagerUtils._

val df = spark.read
.format("com.databricks.spark.redshift")
.option("url", getProdRedshiftUrl)
.option("query", "select * from alefdw_stage.rel_course_activity_container_domain")
.option("tempdir", "s3a://alef-bigdata-ireland/temp_redshift")
.option("forward_spark_s3_credentials", "true")
.option("inferSchema", true)
.option("autoenablessl", "false")
.load()
.orderBy("cacd_dw_id")

val path = "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-container-domain"
df.coalesce(1).write.format("delta").mode("append").save(path)

import com.alefeducation.utils.AWSSecretManagerUtils._

-- Back-filling dim_course_activity_association
INSERT INTO alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
VALUES ('dim_course_activity_association', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(caa_dw_id) from redshift backfill

val path = "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-container-grade-association"
df.coalesce(1).write.format("delta").mode("append").save(path)

-- backfill alefdatalakeproduction.dim_course_activity_container_domain
insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('dim_course_activity_container_domain', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(cacd_dw_id) from redshift backfill


import com.alefeducation.utils.AWSSecretManagerUtils._

val df = spark.read
.format("com.databricks.spark.redshift")
.option("url", getProdRedshiftUrl)
.option("query", "select * from alefdw_stage.rel_course_activity_container_domain")
.option("tempdir", "s3a://alef-bigdata-ireland/temp_redshift")
.option("forward_spark_s3_credentials", "true")
.option("inferSchema", true)
.option("autoenablessl", "false")
.load()
.orderBy("cacd_dw_id")

val path = "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-container-domain"
df.coalesce(1).write.format("delta").mode("append").save(path)

CREATE EXTERNAL TABLE IF NOT EXISTS alefdatalakeproduction.dim_course_activity_association
LOCATION "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-association"
AS
SELECT null::bigint                     AS caa_dw_id,
       plaa_created_time                AS caa_created_time,
       plaa_updated_time                AS caa_updated_time,
       plaa_dw_created_time             AS caa_dw_created_time,
       plaa_dw_updated_time             AS caa_dw_updated_time,
       plaa_status                      AS caa_status,
       plaa_attach_status               AS caa_attach_status,
       plaa_pathway_id                  AS caa_course_id,
       plaa_level_id                    AS caa_container_id,
       plaa_activity_id                 AS caa_activity_id,
       plaa_activity_type               AS caa_activity_type,
       plaa_activity_pacing             AS caa_activity_pacing,
       plaa_activity_index              AS caa_activity_index,
       plaa_pathway_version             AS caa_course_version,
       plaa_is_parent_deleted           AS caa_is_parent_deleted,
       plaa_grade                       AS caa_grade,
       plaa_activity_is_optional        AS caa_activity_is_optional,
       plaa_is_joint_parent_activity    AS caa_is_joint_parent_activity,
       plaa_metadata                    AS caa_metadata
FROM alefdatalakeproduction.dim_pathway_level_activity_association;

-- Back-filling dim_course_activity_outcome_association
insert into alefdatalakeproduction.product_max_ids (table_name, max_id, status, updated_time)
values ('dim_course_activity_outcome_association', MAX_ID_VALUE, 'completed', current_timestamp);
-- where MAX_ID_VALUE taken from max(caoa_dw_id) from redshift backfill


import com.alefeducation.utils.AWSSecretManagerUtils._

val df = spark.read
.format("com.databricks.spark.redshift")
.option("url", getProdRedshiftUrl)
.option("query", "select * from alefdw_stage.rel_course_activity_outcome_association")
.option("tempdir", "s3a://alef-bigdata-ireland/temp_redshift")
.option("forward_spark_s3_credentials", "true")
.option("inferSchema", true)
.option("autoenablessl", "false")
.load()
.orderBy("caoa_dw_id")

val path = "s3://alef-bigdata-ireland/lakehouse/production/alef-ccl-course-activity-outcome-association"
df.coalesce(1).write.format("delta").mode("append").save(path)

update alefdatalakeproduction.fact_adt_next_question set fanq_skill = 'reading' where fanq_class_subject_name = 'Arabic';
update alefdatalakeproduction.fact_adt_student_report set fanq_skill = 'reading' where fasr_class_subject_name = 'Arabic';

select *
FROM alefdatalakeproduction.fact_jira_issue
       where concat(concat(fji_key, fji_created_time), fji_dw_created_time) in
              (SELECT id
              from (
                     SELECT concat(concat(fji_key, fji_created_time), fji_dw_created_time) id,
                     row_number() over (partition by fji_key, fji_created_time order by fji_dw_created_time) as rnk
                     FROM alefdatalakeproduction.fact_jira_issue) a
WHERE a.rnk > 1);

delete
from alefdatalakeproduction.fact_teacher_task_center
              where dw_id in (SELECT a.dw_id
                     FROM (SELECT *,
                            row_number() over (partition by event_id
                            order by
                            dw_created_time) as rnk
                     FROM alefdatalakeproduction.fact_teacher_task_center) a
              WHERE a.rnk > 1);

update alefdatalakeproduction.dim_student set student_username = null where tenant_id = '2746328b-4109-4434-8517-940d636ffa09';