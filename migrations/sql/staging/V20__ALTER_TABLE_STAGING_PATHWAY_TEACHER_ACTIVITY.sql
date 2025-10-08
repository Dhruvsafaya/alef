ALTER TABLE staging_pathway_teacher_activity ADD COLUMN  fpta_activity_type_value VARCHAR(50);
ALTER TABLE staging_pathway_teacher_activity ADD COLUMN fpta_is_added_as_resource bool default false;