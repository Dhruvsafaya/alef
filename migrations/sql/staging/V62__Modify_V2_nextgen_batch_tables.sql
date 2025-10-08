drop table rel_avatar_customization_v2;
drop table rel_avatar_customization;
alter table rel_pathway_target_v2 add column pt_trace_id varchar(36);
alter table staging_item_purchase_v2 add column fip_trace_id varchar(36);
alter table staging_item_purchase_v2 add column fip_event_type varchar(50);
