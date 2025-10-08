-- Alter table to add tenant and School id to all dimensions

alter table alefdw.dim_grade add column grade_school_dw_id BIGINT;
alter table alefdw.dim_grade add column grade_tenant_dw_id BIGINT;

alter table alefdw.dim_class add column class_school_dw_id BIGINT;
alter table alefdw.dim_class add column class_tenant_dw_id BIGINT;

alter table alefdw.dim_subject add column subject_tenant_dw_id BIGINT;

alter table alefdw.dim_user add column user_tenant_dw_id BIGINT;
alter table alefdw.dim_learning_objective add column lo_tenant_dw_id BIGINT;
