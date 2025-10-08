insert into alefdw1.dim_class
  (
            class_created_time,
            class_updated_time,
            class_deleted_time,
            class_dw_created_time,
            class_dw_updated_time,
            class_active_until,
            class_latest_flag,
            class_id,
            class_enabled,
            class_section,
            class_name
  )
  (
  select
            class_create_occurred_on,
            class_update_occurred_on,
            null,
            class_created_date,
            class_updated_date,
            null,
            true,
            class_id,
            class_enabled,
            class_section,
            class_name
  from bigdata_mock.dim_class
  );


insert into alefdw1.dim_grade
  (
            grade_created_time,
            grade_updated_time,
            grade_deleted_time,
            grade_dw_created_time,
            grade_dw_updated_time,
            grade_active_until,
            grade_latest_flag,
            grade_id,
            grade_name
  )
  (
  select
            grade_create_occurred_on,
            grade_update_occurred_on,
            null,
            grade_created_date,
            grade_updated_date,
            null,
            true,
            grade_id,
            grade_name
  from bigdata_mock.dim_grade
  );


insert into alefdw1.dim_school
 (
            school_created_time,
            school_updated_time,
            school_deleted_time,
            school_dw_created_time,
            school_dw_updated_time,
            school_active_until,
            school_latest_flag,
            school_id,
            school_name,
            school_organisation,
            school_address_line,
            school_post_box,
            school_city_name,
            school_country_name,
            school_latitude,
            school_longitude,
            school_first_day,
            school_timezone
 )
 (
 select
            school_create_occurred_on,
            school_update_occurred_on,
            null,
            school_created_date,
            school_updated_date,
            null,
            true,
            school_id,
            school_name,
            school_organization,
            school_address_line,
            school_post_box,
            school_city_name,
            school_country_name,
            school_latitude,
            school_longitude,
            school_first_day,
            school_timezone
  from bigdata_mock.dim_school
  );



insert into alefdw1.dim_subject
 (
            subject_created_time,
            subject_updated_time,
            subject_deleted_time,
            subject_dw_created_time,
            subject_dw_updated_time,
            subject_active_until,
            subject_latest_flag,
            subject_id,
            subject_name,
            subject_online
 )
 (
 select
            subject_create_occurred_on,
            subject_update_occurred_on,
            null,
            subject_created_date,
            subject_updated_date,
            null,
            true,
            subject_id,
            subject_name,
            subject_online
 from bigdata_mock.dim_subject
 );

insert into alefdw1.dim_learning_objective
 (
            lo_created_time,
            lo_updated_time,
            lo_deleted_time,
            lo_dw_created_time,
            lo_dw_updated_time,
            lo_active_until,
            lo_latest_flag,
            lo_id,
            lo_title,
            lo_code,
            lo_type
 )
 (
 select
            lo_update_occurred_on,
            lo_create_occurred_on,
            null,
            lo_created_date,
            lo_updated_date,
            null,
            true,
            lo_id,
            lo_title,
            lo_code,
            lo_type
  from bigdata_mock.dim_learning_objective
  );

insert into alefdw1_stage.rel_user
 (
	          user_id
 )
 (
 select
            user_id
 from bigdata_mock.dim_user
 )

insert into alefdw1_stage.rel_student
 (
	          student_created_time,
	          student_updated_time,
	          student_deleted_time,
	          student_dw_created_time,
	          student_dw_updated_time,
	          student_active_until,
	          student_latest_flag,
	          student_uuid,
	          school_uuid,
	          grade_uuid,
	          class_uuid
 )
 (
 select
            user_create_occurred_on,
            user_update_occurred_on,
            null,
            user_created_date,
            user_updated_date,
            user_active_until,
            user_latest_flag,
            user_id,
            school_id,
            grade_id,
            class_id
 from bigdata_mock.dim_user left join bigdata_mock.dim_school on user_school_dw_id = school_dw_id
 join bigdata_mock.dim_grade on user_grade_dw_id = grade_dw_id
 join bigdata_mock.dim_class on user_class_dw_id = class_dw_id
 where user_role = 2
 );

insert into alefdw1_stage.rel_teacher
 (
             teacher_created_time,
             teacher_updated_time,
             teacher_deleted_time,
             teacher_dw_created_time,
             teacher_dw_updated_time,
             teacher_active_until,
	           teacher_latest_flag,
	           teacher_uuid,
	           subject_uuid
 )
 (
 select
             u.user_create_occurred_on,
             u.user_update_occurred_on,
             null,
             u.user_created_date,
             u.user_updated_date,
             u.user_active_until,
             u.user_latest_flag,
             u.user_id,
             ts.subject_id
 from alefdw1_stage.rel_teacher_subject ts join bigdata_mock.dim_user u
 on ts.teacher_id=u.user_id
 where user_role = 1
 );

insert into alefdw1_stage.rel_guardian
 (
              guardian_created_time,
              guardian_updated_time,
              guardian_deleted_time,
              guardian_dw_created_time,
              guardian_dw_updated_time,
              guardian_active_until,
	            guardian_latest_flag,
              guardian_uuid,
              student_uuid
 )
 (
 select
              u.user_create_occurred_on,
              u.user_update_occurred_on,
              null,
              u.user_created_date,
              u.user_updated_date,
              u.user_active_until,
              u.user_latest_flag,
              u.user_id,
              sg.student_uuid
 from alefdw1_stage.rel_student_guardian sg  join bigdata_mock.dim_user u
     on sg.guardian_uuid = u.user_id
 where user_role = 3
 );


 insert into alefdw1.dim_teacher
(
teacher_created_time,
teacher_updated_time,
teacher_deleted_time,
teacher_dw_created_time,
teacher_dw_updated_time,
teacher_active_until,
teacher_latest_flag,
teacher_id,
teacher_dw_id,
teacher_subject_dw_id
)
Select
rt.teacher_created_time,
rt.teacher_updated_time,
rt.teacher_deleted_time,
rt.teacher_dw_created_time,
rt.teacher_dw_updated_time,
rt.teacher_active_until,
rt.teacher_latest_flag,
ru.user_id,
ru.user_dw_id,
sub.subject_dw_id
from alefdw1_stage.rel_teacher rt
inner join alefdw1_stage.rel_user ru
on rt.teacher_uuid = ru.user_id
inner join alefdw1.dim_subject sub
on rt.subject_uuid = sub.subject_id;

insert into alefdw1.dim_student
(
 student_created_time,
 student_updated_time,
 student_deleted_time,
 student_dw_created_time,
 student_dw_updated_time,
 student_active_until,
 student_latest_flag,
 student_id,
 student_dw_id,
 student_school_dw_id,
 student_grade_dw_id,
 student_class_dw_id
)
(Select

rs.student_created_time,
rs.student_updated_time,
rs.student_deleted_time,
rs.student_dw_created_time,
rs.student_dw_updated_time,
rs.student_active_until,
rs.student_latest_flag,
rs.student_uuid,
ru.user_dw_id,
ds.school_dw_id,
dg.grade_dw_id,
dc.class_dw_id
from alefdw1_stage.rel_student rs
left join alefdw1.dim_school ds
on rs.school_uuid = ds.school_id
inner join alefdw1.dim_class dc
on rs.class_uuid = dc.class_id
inner join alefdw1.dim_grade dg
on rs.grade_uuid = dg.grade_id
inner join alefdw1_stage.rel_user ru
 on rs.student_uuid = ru.user_id
 );

 insert into alefdw1.dim_guardian
(
 guardian_created_time,
 guardian_updated_time,
 guardian_deleted_time,
 guardian_dw_created_time,
 guardian_dw_updated_time,
 guardian_active_until,
 guardian_latest_flag,
 guardian_id,
 guardian_dw_id,
 guardian_student_dw_id
)(
Select
 rg.guardian_created_time,
 rg.guardian_updated_time,
 rg.guardian_deleted_time,
 rg.guardian_dw_created_time,
 rg.guardian_dw_updated_time,
 rg.guardian_active_until,
 rg.guardian_latest_flag,
 ru.user_id,
 ru.user_dw_id,
 ru2.user_dw_id
from alefdw1_stage.rel_guardian rg
inner join alefdw1_stage.rel_user ru
 on rg.guardian_uuid = ru.user_id
inner join alefdw1_stage.rel_user ru2
 on rg.student_uuid = ru2.user_id
 );


-- take school back up for all three tenant in school_bakup_full schema then update student dimension
update alefdw1.dim_student set student_username = bs.username from alefdw1.dim_student join school_backup_full.student bs on student_id =  bs.uuid where bs.uuid = student_id;
update alefdw1_stage.rel_student set student_username = bs.username from alefdw1_stage.rel_student join school_backup_full.student bs on student_uuid =  bs.uuid where bs.uuid = student_uuid;
