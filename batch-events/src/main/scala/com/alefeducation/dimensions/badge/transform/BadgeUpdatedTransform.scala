package com.alefeducation.dimensions.badge.transform
import com.alefeducation.base.SparkBatchService
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform.{BadgeEntityPrefix, BadgeUpdatedIWHCols, BadgeUpdatedParquetSource, BadgeUpdatedTransformed, TenantRedshift}
import com.alefeducation.schema.tenant.Tenant
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.BatchTransformerUtility._
import org.apache.spark.sql.functions.{coalesce, col, explode}
import org.apache.spark.sql.types.IntegerType


class BadgeUpdatedTransform(val session: SparkSession, val service: SparkBatchService) {
  import session.implicits._
  def transform():  List[Option[Sink]] = {

    //Reading data from processing Dir on S3
    val badgeUpdatedSrc: Option[DataFrame] = service.readOptional(BadgeUpdatedParquetSource, session, extraProps = List(("mergeSchema", "true")))

    //Extracting default badges rules columns
    val defaultBadgeRules = badgeUpdatedSrc.map(_.withColumn("outerK12Grades", explode(col("k12Grades")))
      .withColumn("defaultRules", explode(col("rules")))
      .withColumn("tier", col("defaultRules.tier"))
      .withColumn("defaultThreshold", col("defaultRules.defaultThreshold"))
      .withColumn("bdg_tenant_id", col("tenantId"))
      .drop("k12Grades")
      .drop("rules")
      .drop("finalDescription")
      .drop("order")
      .drop("tenantCode"))

    //Extracting per grade rules in a new Dataframe
    val perGradeRules = defaultBadgeRules.map(_.select("id","tier","defaultRules.rulesByGrade")
      .dropDuplicates()
      .withColumn("rulesByGrade", explode(col("rulesByGrade")))
      .withColumn("perGradeId",col("id"))
      .withColumn("perGradeTier",col("tier"))
      .withColumn("perGradeGrade",col("rulesByGrade.k12Grade"))
      .withColumn("perGradeThreshold",col("rulesByGrade.threshold"))
      .drop("id").drop("tier")) //Renamed Columns

    //Joining default with per grade rules to form the final df and get the required threshold per grade
    val finalRules = defaultBadgeRules.map(_.joinOptional(perGradeRules,
      col("id") === col("perGradeId")
      && col("tier") === col("perGradeTier")
      && col("outerK12Grades") === col("perGradeGrade"), "left")
      .addColIfNotExists("perGradeThreshold", IntegerType)//Adding this column to avoid failing in case of perGradeThreshold Absence
      .withColumn("threshold", coalesce(col("perGradeThreshold"),col("defaultThreshold")).cast(IntegerType))
      .withColumn("outerK12Grades", col("outerK12Grades").cast(IntegerType))
    )

    //Reading dim_tenant from Redshift
    val tenants = service.readFromRedshiftOptional[Tenant](TenantRedshift, selectedCols = List("tenant_dw_id", "tenant_id", "tenant_name", "tenant_timezone"))

    //Joining with dim_tenant to get tenant_dw_id for Redshift
    val finalDF = finalRules.map(_.joinOptional(
      tenants, col("tenantId") === col("tenant_id")
      , "left")
    )

    val IWHRules = finalDF.map(
      _.transformForSCD(BadgeUpdatedIWHCols,
        entity= BadgeEntityPrefix,
        List("id", "tier", "outerK12Grades")
      )
    )

    List(
      IWHRules.map(DataSink(BadgeUpdatedTransformed, _))
    )
  }
}

object BadgeUpdatedTransform {

  val BadgeUpdatedParquetSource = "parquet-badge-updated-source"
  val BadgeUpdatedTransformed = "transformed-badge-updated"
  val TenantRedshift = "redshift-tenant-dim"

  val BadgeEntityPrefix = "bdg"
  val BadgeInactiveStatus = 2

  val BadgeUpdatedIWHCols: Map[String, String] = Map[String, String](
    "id" -> "bdg_id",
    "tier" -> "bdg_tier",
    "outerK12Grades" -> "bdg_grade",
    "type" -> "bdg_type",
    "tenant_dw_id" -> "bdg_tenant_dw_id",
    "tenant_id" -> "bdg_tenant_id",
    "title" -> "bdg_title",
    "category" -> "bdg_category",
    "threshold" -> "bdg_threshold",
    "occurredOn" -> "occurredOn",
    "bdg_status" -> "bdg_status"
  )


  val session: SparkSession = SparkSessionUtils.getSession(BadgeUpdatedTransformed)
  val service = new SparkBatchService(BadgeUpdatedTransformed, session)
  def main(args: Array[String]): Unit = {
    val transformer = new BadgeUpdatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
