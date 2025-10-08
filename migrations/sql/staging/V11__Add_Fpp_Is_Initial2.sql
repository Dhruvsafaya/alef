ALTER TABLE staging_pathway_placement ADD COLUMN fpp_is_initial bool default true;

UPDATE staging_pathway_placement
SET fpp_is_initial = false
WHERE fpp_placement_type = 1;
