package com.alefeducation.facts.xapi

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.schema
import com.alefeducation.schema.newxapi.{GuardianExtensions, NewXAPIStatement}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, getStringDateToTimestamp, isEmpty, selectAs}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GuardianXAPITransformer(override val name: String, override val session: SparkSession)
  extends SparkBatchService with XAPIHelper {

  import session.implicits._

  val xApi: StructType = schema[NewXAPIStatement]

  override def transform(): List[Sink] = {
    val rawXApiDf = read(ParquetGuardianXApiV2Source, session)
      .select($"tenantId", $"eventType", from_json($"body", xApi).as("body"), $"loadtime")
      .withColumn("occurredOn", getStringDateToTimestamp("body.timestamp").cast(TimestampType))

    getGuardianAppXApiSink(rawXApiDf)
  }

  def getGuardianAppXApiSink(rawXApiDf: DataFrame): List[Sink] = {
    val guardianXApiDf = rawXApiDf.filter($"eventType" like "guardian.app%")

    if (!isEmpty(guardianXApiDf)) {
      val guardianExtensions: StructType = schema[GuardianExtensions]
      val guardianFlattenDf = getCommonCols(guardianXApiDf, guardianExtensions)
        .withColumn("schoolId", when($"extensions.tenant.school.id" isNull, DEFAULT_ID).otherwise($"extensions.tenant.school.id"))
        .withColumn("studentId", when($"extensions.student.id" isNull, DEFAULT_ID).otherwise($"extensions.student.id"))
        .withColumn("device", $"extensions.device")
        .withColumn("timestampLocal", substring($"timestampLocal", StartingIndexTimestampLocal, EndIndexTimestampLocal))

      val guardianRedshiftDf: DataFrame =
        addTimestampColumns(selectAs(guardianFlattenDf, guardianXAPIColumnMapping), FactGuardianAppActivities, isFact = true)

      val guardianParquetSink: DataSink = DataSink(
        ParquetGuardianXApiV2Source,
        guardianXApiDf.withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd")).coalesce(1),
        Map("partitionBy" -> "eventdate,eventType")
      )

      import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
      import com.alefeducation.util.BatchTransformerUtility._

      val deltaSink: Option[Sink] = guardianRedshiftDf
        .renameUUIDcolsForDelta(Some("fgaa_"))
        .withColumn("eventdate", date_format(col("fgaa_created_time"), "yyyy-MM-dd"))
        .toCreate(isFact = true)
        .map(_.toSink(DeltaGuardianAppActivitiesSink, eventType = "guardianApp"))

      val guardianAppActivitiesSink: List[Sink] =
        List(guardianParquetSink, DataSink(RedshiftGuardianAppActivitiesSink, guardianRedshiftDf)) ++ deltaSink

      guardianAppActivitiesSink
    } else Nil
  }
}

object GuardianXAPI {
  def main(args: Array[String]): Unit = {
    new GuardianXAPITransformer(GuardianXapiV2Service, SparkSessionUtils.getSession(GuardianXapiV2Service)).run
  }
}
