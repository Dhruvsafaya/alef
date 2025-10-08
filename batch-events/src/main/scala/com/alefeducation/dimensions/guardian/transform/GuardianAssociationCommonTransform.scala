package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.ParquetGuardianAssociationsSource
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{explode, lit, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GuardianAssociationCommonTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    val association = service.readOptional(ParquetGuardianAssociationsSource, session)

    val associationCommon = association
      .map(_.transform(_.selectLatestByRowNumber(List("studentId"))))
      .flatMap(associationToCommonSchema)

    associationCommon.map(df => { DataSink(GuardianAssociationCommonTransformSink, df) })
  }

  private def associationToCommonSchema(df: DataFrame): Option[DataFrame] = {
    val nonEmptyGuardianDf = df.filter(size($"guardians") > 0).cache
    if (!nonEmptyGuardianDf.isEmpty) {
      Some(
        nonEmptyGuardianDf.transform { df =>
          val explodedGuardians = df.select(
            explode($"guardians.id").as("guardianId"),
            $"studentId",
            $"occurredOn",
            $"eventType"
          )
          val latestRecords = explodedGuardians.selectLatestByRowNumber(List("studentId", "guardianId"))
          latestRecords
            .withColumn("status", lit(1))
            .withColumn("invitationStatus", lit(2))
            .drop("eventType")
        }
      )
    } else None

  }

}
object GuardianAssociationCommonTransform {
  val GuardianAssociationCommonTransformService = "transform-guardian-association-common"
  val GuardianAssociationCommonTransformSink = "transformed-guardian-association-common-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianAssociationCommonTransformService)
  val service = new SparkBatchService(GuardianAssociationCommonTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianAssociationCommonTransform(session, service)
    service.run(transformer.transform())
  }

}
