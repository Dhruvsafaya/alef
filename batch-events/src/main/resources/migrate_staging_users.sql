drop table if exists tempTable;

create table tempTable as
  select ful_login_created,
         ful_date_dw_id,
         ful_id,
         user_id,
         role_name,
         tenant_dw_id,
         school_id,
         ful_outside_of_school
  from bigdata_mock.fact_user_login
         right join bigdata_mock.dim_user on user_dw_id = ful_user_dw_id
         join bigdata_mock.dim_role on role_dw_id = user_role
         left join bigdata_mock.dim_tenant t on tenant_dw_id = ful_tenant_dw_id
         left join bigdata_mock.dim_school s on school_dw_id = ful_school_dw_id;

insert into alefdw1_stage.staging_user_login (ful_created_time,
                                             ful_date_dw_id,
                                             ful_id,
                                             user_uuid,
                                             role_uuid,
                                             tenant_uuid,
                                             school_uuid,
                                             ful_outside_of_school) (select * from tempTable);


UPDATE alefdw1_stage.staging_user_login
SET ful_dw_created_time = ful_created_time, ful_login_time = ful_created_time;
