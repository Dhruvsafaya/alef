ALTER TABLE staging_level_completed ADD COLUMN flc_academic_year VARCHAR(50);
ALTER TABLE staging_level_completed ADD COLUMN flc_score INTEGER;
ALTER TABLE staging_levels_recommended ADD COLUMN flr_academic_year VARCHAR(50);
ALTER TABLE staging_pathway_activity_completed ADD COLUMN fpac_academic_year VARCHAR(50);
