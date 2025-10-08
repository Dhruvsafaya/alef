UPDATE alefdw.dim_pathway_target set pt_target_dw_id = pt.dw_id
FROM
alefdw.dim_pathway_target dpt JOIN alefdw_stage.rel_dw_id_mappings pt
  ON dpt.pt_target_id = pt.id
WHERE pt.entity_type = 'pathway_target' and dpt.pt_target_dw_id IS NULL;
