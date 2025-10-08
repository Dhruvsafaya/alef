alter table dim_school add column school_content_repository_id varchar(36);
alter table dim_school add column school_organization_code varchar(50);
alter table dim_school add column _is_complete boolean default false;