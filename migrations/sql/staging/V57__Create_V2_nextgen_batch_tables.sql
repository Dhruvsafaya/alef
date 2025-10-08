CREATE TABLE rel_avatar_customization_v2 (
  ac_dw_id BIGINT NOT NULL,
  ac_created_time TIMESTAMP,
  ac_updated_time TIMESTAMP,
  ac_deleted_time TIMESTAMP,
  ac_dw_created_time TIMESTAMP,
  ac_dw_updated_time TIMESTAMP,
  ac_status INT,
  ac_student_id VARCHAR(36),
  ac_avatar_id VARCHAR(36),
  ac_type VARCHAR(30),
  ac_item_id VARCHAR(36),
  ac_item_type VARCHAR(30),
  ac_item_value VARCHAR(60)
);

CREATE TABLE IF NOT EXISTS staging_item_purchase_v2 (
  fip_dw_id BIGINT,
  fip_created_time TIMESTAMP,
  fip_dw_created_time TIMESTAMP,
  fip_date_dw_id BIGINT,
  fip_id VARCHAR(36),
  fip_item_id VARCHAR(36),
  fip_item_type VARCHAR(50),
  fip_item_title VARCHAR(50),
  fip_item_description VARCHAR(50),
  fip_transaction_id VARCHAR(36),
  fip_school_id VARCHAR(36),
  fip_grade_id VARCHAR(36),
  fip_section_id VARCHAR(36),
  fip_academic_year_id VARCHAR(36),
  fip_academic_year INTEGER,
  fip_student_id VARCHAR(36),
  fip_tenant_id VARCHAR(36),
  fip_redeemed_stars INTEGER
);

CREATE TABLE rel_pathway_target_v2
(
  rel_pt_dw_id  bigint,
  pt_dw_id  bigint,
  pt_id VARCHAR(36),
  pt_created_time Timestamp,
  pt_dw_created_time  Timestamp,
  pt_status  Int,
  pt_active_until  Timestamp,
  pt_target_id  VARCHAR(36),
  pt_target_state VARCHAR(20),
  pt_start_date VARCHAR(10),
  pt_end_date VARCHAR(10),
  pt_tenant_id  VARCHAR(36),
  pt_school_id  VARCHAR(36),
  pt_grade_id VARCHAR(36),
  pt_class_id VARCHAR(36),
  pt_teacher_id VARCHAR(36),
  pt_pathway_id VARCHAR(36)
);

create table rel_dw_id_mappings_v2
(
dw_id                  bigint identity (1,1),
id                     varchar(36),
entity_type            varchar(50),
entity_dw_created_time timestamp,
entity_created_time    timestamp
)
sortkey (entity_type);
