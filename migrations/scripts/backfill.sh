REDSHIFT_BACKFILLING_DB_SCHEMA=alefdw
BACKFILLING_SCRIPTS_FOLDER=migrations/sql/backfill

echo "BACKFILLING"
echo "Redshift Connection String:  " $REDSHIFT_DB_URL
echo "Redshift Username:           " $REDSHIFT_DB_USERNAME
echo "Redshift Schema :            " $REDSHIFT_BACKFILLING_DB_SCHEMA
echo "Using migration script from: " $BACKFILLING_SCRIPTS_FOLDER

flyway-7.15.0/flyway migrate \
  -url=$REDSHIFT_DB_URL \
  -user=$REDSHIFT_DB_USERNAME \
  -password=$REDSHIFT_DB_PASSWORD \
  -locations=$BACKFILLING_SCRIPTS_FOLDER \
  -schemas=$REDSHIFT_BACKFILLING_DB_SCHEMA \
  -table=flyway_backfill_history \
  -baselineOnMigrate=true \
  -outOfOrder=true \
  -community
