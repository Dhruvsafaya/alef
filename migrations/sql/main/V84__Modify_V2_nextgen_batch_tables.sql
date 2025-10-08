alter table dim_avatar_v2 add column avatar_trace_id varchar(36);
alter table dim_avatar_v2 add column avatar_event_type varchar(50);
alter table dim_avatar_layer_customization_v2 add column ala_trace_id varchar(36);
alter table dim_avatar_layer_customization_v2 add column ala_event_type varchar(50);
alter table dim_pathway_target_v2 add column pt_trace_id varchar(36);
alter table fact_item_purchase_v2 add column fip_trace_id varchar(36);
alter table fact_item_purchase_v2 add column fip_event_type varchar(50);
