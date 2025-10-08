-- COMPARE SCHOOL UPDATES
DROP TABLE if exists school_updates_missing;
CREATE TEMP TABLE school_updates_missing AS (
(select school_uuid, organization, address_line, post_box, city_name, country_name, first_day, timezone from school_backup_full_test.school)
minus
(select school_id, school_organization, school_address_line, school_post_box, school_city_name, school_country_name, school_first_day, school_timezone
 from bigdata_mock.dim_school));

select * from school_updates_missing;
------------------------------------------------------------------------------------------------------------------------------------------------------------

-- COMPARE GRADE UPDATES
drop table if exists grades_missing;
create temp table grades_missing as (select uuid, name from school_backup_full_test.grade where school_uuid is not NULL
minus
select grade_id, grade_name from bigdata_mock.dim_grade);
select * from grades_missing;

select * from bigdata_mock.dim_grade where grade_id in (select uuid from grades_missing);
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- COMPARE CLASS UPDATES
drop table if exists classes_missing;

create temp table classes_missing as ((select uuid, name, section from school_backup_full_test.class)
minus
(select class_id, class_name, class_section from bigdata_mock.dim_class));

select * from classes_missing missed join bigdata_mock.dim_class c on missed.uuid = c.class_id;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- COMPARE SUBJECT UPDATES
drop table if exists subject_missing;

drop table if exists backup_subject;
create temp table backup_subject as (
       select subject_id, subject_name, online, school_dw_id, grade_dw_id, experiential_content, curriculum_id, curriculum_name,
              curriculum_grade_id, curriculum_subject_id, curriculum_grade_name, curriculum_subject_name
 from school_backup_full_test.subject s
        join bigdata_mock.dim_school bsc on s.school_uuid = bsc.school_id
        join bigdata_mock.dim_grade bg on s.grade_uuid = bg.grade_id);

select * from backup_subject;

drop table if exists redshift_subject;
create temp table redshift_subject as (select subject_id, subject_name, subject_online, subject_school_dw_id, subject_grade_dw_id, subject_experiential_content,
       subject_curriculum_id, subject_curriculum_name,
       subject_curriculum_grade_id, subject_curriculum_subject_id, subject_curriculum_grade_name, subject_curriculum_subject_name
from bigdata_mock.dim_subject);

create temp table subject_missing as (select * from backup_subject except select * from redshift_subject);
select * from subject_missing miss join bigdata_mock.dim_subject s on miss.subject_id = s.subject_id;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- COMPARE STUDENT UPDATES
DROP TABLE if exists student_updates_missing;
CREATE TEMP TABLE student_updates_missing AS
((select uuid, school_dw_id, grade_dw_id, class_dw_id from school_backup_full_test.student s
  join bigdata_mock.dim_class bc on class_uuid = bc.class_id
  join bigdata_mock.dim_grade bg on grade_uuid = bg.grade_id
  join bigdata_mock.dim_school bs on school_uuid = bs.school_id) minus
    (
    select user_id, user_school_dw_id, user_grade_dw_id, user_class_dw_id  from bigdata_mock.dim_user
  where user_role = 2));

select * from student_updates_missing missing join bigdata_mock.dim_user u on missing.uuid = user_id;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- COMPARE TEACHER UPDATES
DROP TABLE if exists teacher_updates_missing;
CREATE TEMP TABLE teacher_updates_missing AS
((select uuid, school_dw_id from school_backup_full_test.teacher t
  join bigdata_mock.dim_school bs on school_uuid = bs.school_id) minus
    (select user_id, user_school_dw_id from bigdata_mock.dim_user
  where user_role = 1));

select * from teacher_updates_missing;
------------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------------***************************************-------------------------------------------------------------------------------
----------                                          UPDATES
--------------------------------------***************************************-------------------------------------------------------------------------------

-- UPDATE SCHOOL IF FOUND MISSING
update bigdata_mock.dim_school set (school_id, school_organization, school_address_line, school_post_box, school_city_name,
    school_country_name, school_first_day, school_timezone) =
    (school_uuid, organization, address_line, post_box, city_name, country_name, first_day, timezone)
from school_updates_missing s where school_id = s.school_uuid;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- UPDATE GRADE IF FOUND MISSING
update bigdata_mock.dim_grade set grade_name = g.name from grades_missing g where grade_id = g.uuid;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- UPDATE CLASS IF FOUND MISSING
update bigdata_mock.dim_class set class_name= c.name, class_section = c.section
from classes_missing c where class_id = c.uuid;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- UPDATE SUBJECT IF FOUND MISSING
update bigdata_mock.dim_subject set
subject_name = sub.subject_name,
subject_online = sub.online,
subject_school_dw_id = sub.school_dw_id,
subject_grade_dw_id = sub.grade_dw_id,
subject_experiential_content = sub.experiential_content,
subject_curriculum_id = sub.curriculum_id,
subject_curriculum_name = sub.curriculum_name,
subject_curriculum_grade_id = sub.curriculum_grade_id,
subject_curriculum_subject_id = sub.curriculum_subject_id,
subject_curriculum_grade_name = sub.curriculum_grade_name,
subject_curriculum_subject_name = sub.curriculum_subject_name
from subject_missing sub where dim_subject.subject_id = sub.subject_id;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- UPDATE STUDENT IF FOUND MISSING
update bigdata_mock.dim_user SET
user_school_dw_id = s.school_dw_id,
user_grade_dw_id = s.grade_dw_id,
user_class_dw_id = s.class_dw_id
from student_updates_missing s where user_id = s.uuid;
------------------------------------------------------------------------------------------------------------------------------------------------------------


-- UPDATE TEACHER IF FOUND MISSING
update bigdata_mock.dim_user SET user_school_dw_id = t.school_dw_id
from teacher_updates_missing t where user_id = t.uuid;
------------------------------------------------------------------------------------------------------------------------------------------------------------