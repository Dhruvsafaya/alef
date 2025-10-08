--ALEF_72247 backfill school_organization_code, school_content_repository_id and _is_complete column in dim_school

update alefdw.dim_school set
school_content_repository_id = cp.school_content_repository_id,
school_organization_code = cp.school_organization_id,
_is_complete = false
from alefdw.dim_school sc join backups.school_null_org_content_repo cp on sc.school_id = cp.school_id
where sc.school_content_repository_dw_id is null
or sc.school_organization_dw_id is null;

update alefdw.dim_school
set school_organization_code       = o.organization_code
from alefdw.dim_school sc
inner join alefdw.dim_organization o on sc.school_content_repository_dw_id = o.organization_dw_id
where sc.school_organization_dw_id is not null;

update alefdw.dim_school
set school_content_repository_id = c.id
from alefdw.dim_school sc
inner join alefdw_stage.rel_dw_id_mappings c on sc.school_content_repository_dw_id = c.dw_id and c.entity_type = 'content-repository'
where sc.school_content_repository_dw_id is not null;

--Update _is_complete to true when old records where school_organization_code and school_content_repository_id is not present
-- and mark it true when the reference ids are filled
update alefdw.dim_school
set _is_complete = true
where
(school_organization_dw_id is not null and school_content_repository_dw_id is not null)
or
(school_organization_code is null and school_content_repository_id is null);
