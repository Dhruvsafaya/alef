ALTER TABLE fact_pathway_placement ADD COLUMN fpp_is_initial bool default true;

UPDATE fact_pathway_placement
SET fpp_is_initial = false
WHERE fpp_placement_type = 1;