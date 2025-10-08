echo "####### Setting up flyway for redshift #######"

echo "Downloading flyway commandline"
curl -o flyway-commandline-7.15.0.tar.gz https://repo1.maven.org/maven2/org/flywaydb/flyway-commandline/7.15.0/flyway-commandline-7.15.0.tar.gz
tar -xzf flyway-commandline-7.15.0.tar.gz

echo "Downloading redshift jdbc jar"
curl -o redshift-jdbc42-2.1.0.28.jar https://redshift-downloads.s3.amazonaws.com/drivers/jdbc/2.1.0.28/redshift-jdbc42-2.1.0.28.jar
cp redshift-jdbc42-2.1.0.28.jar flyway-7.15.0/drivers/.

rm flyway-commandline-7.15.0.tar.gz
rm redshift-jdbc42-2.1.0.28.jar
echo "Removed downloaded files"