echo "MIGRATIONS TO STAGING TABLES"
echo "Redshift Connection String:  " $REDSHIFT_DB_URL
echo "Redshift username:           " $REDSHIFT_DB_USERNAME
echo "Redshift Schema :            " $REDSHIFT_STAGING_DB_SCHEMA
echo "Using migration script from: " migrations/sql/staging

flyway-7.15.0/flyway migrate -url=$REDSHIFT_DB_URL -user=$REDSHIFT_DB_USERNAME -password=$REDSHIFT_DB_PASSWORD -locations=migrations/sql/staging -schemas=$REDSHIFT_STAGING_DB_SCHEMA -table=flyway_schema_history -baselineOnMigrate=true -outOfOrder=true -community

echo "MIGRATIONS TO MAIN TABLES"
echo "Redshift Schema :            " $REDSHIFT_MAIN_DB_SCHEMA
echo "Using migration script from: " migrations/sql/main

flyway-7.15.0/flyway migrate -url=$REDSHIFT_DB_URL -user=$REDSHIFT_DB_USERNAME -password=$REDSHIFT_DB_PASSWORD -locations=migrations/sql/main -schemas=$REDSHIFT_MAIN_DB_SCHEMA -table=flyway_schema_history -baselineOnMigrate=true -outOfOrder=true -community