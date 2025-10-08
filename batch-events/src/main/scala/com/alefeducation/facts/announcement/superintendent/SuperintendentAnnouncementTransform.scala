package com.alefeducation.facts.announcement.superintendent

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class SuperintendentAnnouncementTransform(val session: SparkSession, val service: SparkBatchService) {

  import SuperintendentAnnouncementTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val superintendentDf = service.readOptional(ParquetSuperintendentAnnouncementSource, session)

    val accumulator: Option[DataFrame] = None
    val transformed = recipientColumnNames.foldLeft(accumulator) { (tempDF, recipientColName) =>
      val transformedRecipient = superintendentDf.map(
        _.filter(col(recipientColName).isNotNull)
          .transform(
            explodeRecipients(recipientColName) _ compose
              createRecipientTypeId(recipientColName) compose
              createRecipientTypeDesc(recipientColName) compose
              addStatus(ActiveStatus) compose
              addRoleName(SuperintendentRoleName) compose
              addType
          )
          .transformForInsertFact(SuperintendentAnnouncementCols, AnnouncementEntity)
      )
      tempDF.unionOptionalByNameWithEmptyCheck(transformedRecipient)
    }

    transformed.map(DataSink(SuperintendentAnnouncementTransformedSink, _))
  }
}

object SuperintendentAnnouncementTransform {

  val SuperintendentAnnouncementTransformService = "transform-superintendent-announcement"
  val ParquetSuperintendentAnnouncementSource = "parquet-superintendent-announcement-source"
  val SuperintendentAnnouncementTransformedSink = "superintendent-announcement-transformed-sink"

  val session = SparkSessionUtils.getSession(SuperintendentAnnouncementTransformService)
  val service = new SparkBatchService(SuperintendentAnnouncementTransformService, session)

  val SuperintendentAnnouncementCols: Map[String, String] =
    CommonAnnouncementCols ++ Map("superintendentId" -> "fa_admin_id")

  val recipientColumnNames: Seq[String] = Seq(SchoolIdsColName, GradeIdsColName)

  def main(args: Array[String]): Unit = {
    val transform = new SuperintendentAnnouncementTransform(session, service)
    service.run(transform.transform())
  }
}
