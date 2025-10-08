package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.school.transform.SchoolOrganizationTransform.SchoolOrganizationTransformSink
import com.alefeducation.dimensions.school.transform.SchoolUpdateCombinedTransform.{SchoolDeletedTransformSink, SchoolStatusUpdatedTransformSink, SchoolUpdatedTransformSink}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{AdminSchoolDeleted, AdminSchoolUpdated, SchoolActivated}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SchoolUpdateCombinedTransform(val session: SparkSession, val service: SparkBatchService) {
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  /**
    * At one place, transform all events updating existing records.
    * We are combining multiple sinks here as we update non-SCD table, so the final state of the table
    * will be same regardless of duplicates in transformed data caused by job re-runs.
    * @return
    */
  def transform(): List[Option[DataSink]] = {
    val schoolWithOrganization: Option[DataFrame] = service.readOptional(SchoolOrganizationTransformSink, session)
    val schoolStatus: Option[DataFrame] = service.readOptional(ParquetSchoolStatusSource, session)

    val updatedSink: Option[DataSink] = transformUpdated(schoolWithOrganization)
    val statusUpdatedSink: Option[DataSink] = transformStatusUpdated(schoolStatus)
    val statusDeletedSink: Option[DataSink] = transformDeleted(schoolWithOrganization)

    List(updatedSink, statusUpdatedSink, statusDeletedSink)
  }

  private def transformUpdated(schoolWithOrganization: Option[DataFrame]): Option[DataSink] = {
    val SchoolDimensionColumnsWithoutStatus = SchoolDimensionCols.-("school_status")
    val updated = schoolWithOrganization.flatMap(
      _.filter($"eventType" === AdminSchoolUpdated).checkEmptyDf
        .map(_.transformForUpdateDim(SchoolDimensionColumnsWithoutStatus, SchoolEntity, ids = List("uuid")))
    )

    updated.map(df => { DataSink(SchoolUpdatedTransformSink, df) })
  }

  private def transformStatusUpdated(schoolStatus: Option[DataFrame]): Option[DataSink] = {
    val status = schoolStatus.flatMap(
      _.transformForUpdateDim(SchoolDimensionStatusCols, SchoolEntity, ids = List("schoolId")).checkEmptyDf
        .map(
          _.withColumn(
            "school_status",
            when($"eventType" === SchoolActivated, lit(ActiveEnabled)).otherwise(lit(Disabled))
          ).drop($"eventType")
        )
    )

    status.map(df => { DataSink(SchoolStatusUpdatedTransformSink, df) })
  }

  private def transformDeleted(schoolWithOrganization: Option[DataFrame]): Option[DataSink] = {
    val deleted = schoolWithOrganization.flatMap(
      _.filter($"eventType" === AdminSchoolDeleted).checkEmptyDf
        .map(_.transformForDelete(SchoolDimensionCols, SchoolEntity, idColumns = List("uuid")))
    )

    deleted.map(df => { DataSink(SchoolDeletedTransformSink, df) })
  }

}
object SchoolUpdateCombinedTransform {
  val SchoolUpdateCombinedService = "transform-school-update-combined"

  val SchoolUpdatedTransformSink = "transformed-school-updated-sink"
  val SchoolStatusUpdatedTransformSink = "transformed-school-status-updated-sink"
  val SchoolDeletedTransformSink = "transformed-school-deleted-sink"

  val session: SparkSession = SparkSessionUtils.getSession(SchoolUpdateCombinedService)
  val service = new SparkBatchService(SchoolUpdateCombinedService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new SchoolUpdateCombinedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
