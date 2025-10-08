import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/* This script helps to prepare data for dim_organisation, dim_term and dim_week table by consuming the source table.
  It is not fully automated. Can be extended further to ingest data directly to Redshift/Delta
 */
val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("Org Data Generator")
  .getOrCreate()

val termsWeeksPath = "/path/to/terms_week.json"
val orgsJsonPath = "/path/to/org.json"

import spark.implicits._
val orgs = (spark.read.option("inferSchema", true).option("multiLine", true).option("mode", "PERMISSIVE")
  .json(orgsJsonPath)
  .select(
    $"id".as("organisation_id"),
    $"name".as("organisation_name"),
    $"dw_id".as("organisation_dw_id"),
    lit(1).as("organisation_status")
  ))
orgs.show(200, false)

val termsWeeks = spark.read.option("inferSchema", true).option("multiLine", true).option("mode", "PERMISSIVE").json(termsWeeksPath)

def getTerms(sourceData: DataFrame, orgUuid: String, orgDwId: Int): DataFrame = {
  val result = sourceData.select(
    $"createdOn".as("term_created_time"),
    current_timestamp().as("term_dw_created_time"),
    lit(1).as("term_status"),
    $"id".as("term_id"),
    $"number".as("term_academic_period_order"),
    lit(392027).as("term_curriculum_id"),
    lit(4).as("term_content_academic_year_id"),
    lit(orgUuid).as("term_organisation_id"),
    to_date($"startDate", "yyyy-MM-dd").as("term_start_date"),
    to_date($"endDate", "yyyy-MM-dd").as("term_end_date"),
    lit(orgDwId).as("term_organisation_dw_id")
  )
  result.select(to_csv(struct(result.columns.head, result.columns.tail:_*)))
}

val terms = getTerms(termsWeeks, "d5a0f9da-6cb7-4465-bf83-0681850ea9fe", 19)
terms.show(200, false)


def getWeeks(sourceRecords: DataFrame): DataFrame = {
  val result = sourceRecords
    .select($"id", explode($"weeks").as("week"))
    .coalesce(1)
    .select(
      $"week.createdOn".as("week_created_time"),
      current_timestamp().as("week_dw_created_time"),
      lit(1).as("week_status"),
      $"week.id".as("week_id"),
      $"week.number".as("week_number"),
      to_date($"week.startDate", "yyyy-MM-dd").as("week_start_date"),
      date_add(to_date($"week.startDate", "yyyy-MM-dd"), 6).as("week_end_date"),
      $"id".as("week_term_id"),
    )
  result.select(to_csv(struct(result.columns.head, result.columns.tail:_*)))
}

getWeeks(termsWeeks).show(200, false)