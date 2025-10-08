update alefdw.dim_pathway_target
set pt_pathway_dw_id=m.dw_id from alefdw.dim_pathway_target p
join alefdw_stage.rel_dw_id_mappings m
on m.id = p.pt_pathway_id and m.entity_type='course';

update alefdw.fact_pathway_target_progress
set fptp_pathway_dw_id=m.dw_id from alefdw.fact_pathway_target_progress p
join alefdw_stage.rel_dw_id_mappings m
on m.id = p.fptp_pathway_id and m.entity_type='course';