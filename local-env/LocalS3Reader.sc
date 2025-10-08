import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("SparkTest")
  .master("local[*]")
  .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
  .config("spark.hadoop.fs.s3a.endpoint", "localhost:4566")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .getOrCreate()

val incoming = spark.read.parquet("s3a://alef-bigdata-emr/alef-data-platform/alef-23829/incoming/alef-cx-maturity/*")
incoming.show(false)
