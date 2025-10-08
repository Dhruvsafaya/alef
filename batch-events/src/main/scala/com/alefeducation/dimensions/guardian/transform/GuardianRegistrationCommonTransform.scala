package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationCommonTransform.GuardianRegistrationCommonTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.AdminGuardianRegistered
import com.alefeducation.util.Helpers.{ActiveEnabled, GuardianRegistered, MinusOne, ParquetGuardianSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType

class GuardianRegistrationCommonTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): Option[DataSink] = {
    val registered = service.readOptional(ParquetGuardianSource, session)

    // Transform registration event to common schema
    val registeredCommon = registered.map(
      _.withColumn("invitationStatus",
                   when(
                     col("eventType") === AdminGuardianRegistered,
                     lit(GuardianRegistered)
                   ).otherwise(lit(MinusOne)))
        .withColumn("status", lit(ActiveEnabled))
        .withColumn("studentId", lit(null).cast(StringType))
        .withColumnRenamed("uuid", "guardianId")
    )

    registeredCommon.map(df => { DataSink(GuardianRegistrationCommonTransformSink, df) })
  }
}

object GuardianRegistrationCommonTransform {
  val GuardianRegistrationCommonTransformService = "transform-guardian-registration-common"
  val GuardianRegistrationCommonTransformSink = "transformed-guardian-registration-common-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianRegistrationCommonTransformService)
  val service = new SparkBatchService(GuardianRegistrationCommonTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianRegistrationCommonTransform(session, service)
    service.run(transformer.transform())
  }

}
