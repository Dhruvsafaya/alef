package com.alefeducation.facts.announcement.deleted

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

class AnnouncementDeletedTransform(val session: SparkSession, val service: SparkBatchService) {

  import AnnouncementDeletedTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {

    val deletedDf = service.readUniqueOptional(
      ParquetAnnouncementDeletedSource, session, uniqueColNames=Seq("announcementId")
    )
    val announcementDf = service.readOptional(DeltaAnnouncementSource, session)

    val transformed = deletedDf
      .map(
        _.joinOptional(announcementDf,
          col("announcementId") === col("fa_id")
            && col("fa_status") === lit(ActiveStatus),
          "inner")
          .transform(addStatus(DeletedStatus))
          .transformForInsertFact(AnnouncementDeletedCols, AnnouncementEntity)
      )

    transformed.map(DataSink(AnnouncementDeletedTransformedSink, _))
  }
}

object AnnouncementDeletedTransform {

  val AnnouncementDeletedTransformService = "transform-announcement-deleted"
  val ParquetAnnouncementDeletedSource = "parquet-announcement-deleted-source"
  val AnnouncementDeletedTransformedSink = "announcement-deleted-transformed-sink"

  val session = SparkSessionUtils.getSession(AnnouncementDeletedTransformService)
  val service = new SparkBatchService(AnnouncementDeletedTransformService, session)

  val AnnouncementDeletedCols = Map(
    "fa_tenant_id" -> "fa_tenant_id",
    "fa_id" -> "fa_id",
    "fa_recipient_id" -> "fa_recipient_id",
    "fa_has_attachment" -> "fa_has_attachment",
    "fa_status" -> "fa_status",
    "fa_recipient_type" -> "fa_recipient_type",
    "fa_recipient_type_description" -> "fa_recipient_type_description",
    "fa_role_id" -> "fa_role_id",
    "fa_admin_id" -> "fa_admin_id",
    "fa_type" -> "fa_type",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transform = new AnnouncementDeletedTransform(session, service)
    service.run(transform.transform())
  }
}