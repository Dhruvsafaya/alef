-- Grants given to flyway migration user
GRANT CREATE ON SCHEMA alefdw_stage TO flyway_migration_prd;
GRANT USAGE ON SCHEMA alefdw_stage TO flyway_migration_prd;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw_stage TO flyway_migration_prd;

GRANT CREATE ON SCHEMA alefdw TO flyway_migration_prd;
GRANT USAGE ON SCHEMA alefdw TO flyway_migration_prd;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw TO flyway_migration_prd;

GRANT CREATE ON SCHEMA backups TO flyway_migration_prd;
GRANT USAGE ON SCHEMA backups TO flyway_migration_prd;
GRANT ALL ON ALL TABLES IN SCHEMA backups TO flyway_migration_prd;

-- appuser group is granted all for alefdw_stage and granted all except delete for alefdw
GRANT USAGE ON SCHEMA alefdw_stage TO GROUP appuser;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw_stage TO GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage GRANT ALL ON TABLES TO GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage FOR USER flyway_migration_prd GRANT ALL ON TABLES TO GROUP appuser;

GRANT ALL ON SCHEMA alefdw TO GROUP appuser;
GRANT ALL ON ALL TABLES IN SCHEMA alefdw TO GROUP appuser;
REVOKE DELETE ON ALL TABLES IN SCHEMA alefdw FROM GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT ALL ON TABLES TO GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT ALL ON TABLES TO GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw REVOKE DELETE ON TABLES FROM GROUP appuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd REVOKE DELETE ON TABLES FROM GROUP appuser;

-- data_engineering group is granted all except delete for alefdw_stage and alefdw
GRANT USAGE ON SCHEMA alefdw_stage TO GROUP data_engineering;
REVOKE ALL ON ALL TABLES IN SCHEMA alefdw_stage FROM GROUP data_engineering;
GRANT SELECT ON ALL TABLES IN SCHEMA alefdw_stage TO GROUP data_engineering;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage GRANT SELECT ON TABLES TO GROUP data_engineering;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP data_engineering;

REVOKE ALL ON SCHEMA alefdw FROM GROUP data_engineering;
GRANT USAGE ON SCHEMA alefdw TO GROUP data_engineering;
REVOKE ALL ON ALL TABLES IN SCHEMA alefdw FROM GROUP data_engineering;
GRANT SELECT ON ALL TABLES IN SCHEMA alefdw TO GROUP data_engineering;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT SELECT ON TABLES TO GROUP data_engineering;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP data_engineering;

-- tdc group is granted only select for alefdw_stage and alefdw
GRANT USAGE ON schema alefdw TO  GROUP tdc;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP tdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT SELECT ON TABLES TO GROUP tdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP tdc;

-- datascience group is granted only select for alefdw_stage and alefdw
GRANT USAGE ON schema alefdw TO GROUP datascience;
GRANT SELECT ON ALL TABLES IN schema alefdw TO GROUP datascience;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT SELECT ON TABLES TO GROUP datascience;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP datascience;

-- business_intelligence group is granted only select for alefdw_stage and alefdw
GRANT USAGE ON schema  alefdw TO  GROUP business_intelligence;
GRANT SELECT ON ALL TABLES IN  schema alefdw  TO  GROUP business_intelligence;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT SELECT ON TABLES TO GROUP business_intelligence;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP business_intelligence;


-- ro_users group is granted only select for alefdw_stage and alefdw
GRANT USAGE ON schema alefdw TO GROUP ro_users;
GRANT SELECT ON ALL TABLES IN schema alefdw TO GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw GRANT SELECT ON TABLES TO GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP ro_users;


GRANT USAGE ON schema alefdw_stage TO GROUP ro_users;
GRANT SELECT ON ALL TABLES IN schema alefdw_stage TO GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage GRANT SELECT ON TABLES TO GROUP ro_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alefdw_stage FOR USER flyway_migration_prd GRANT SELECT ON TABLES TO GROUP ro_users;