package com.alefeducation.facts.announcement.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class TeacherAnnouncementTransform(val session: SparkSession, val service: SparkBatchService) {

  import TeacherAnnouncementTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val teacherDf = service.readOptional(ParquetTeacherAnnouncementSource, session)

    val accumulator: Option[DataFrame] = None
    val transformed = recipientColumnNames.foldLeft(accumulator) { (tempDF, recipientColName) =>
      val transformedRecipient = teacherDf.map(
        _.filter(col(recipientColName).isNotNull)
          .transform(
            explodeRecipients(recipientColName) _ compose
              createRecipientTypeId(recipientColName) compose
              createRecipientTypeDesc(recipientColName) compose
              addStatus(ActiveStatus) compose
              addRoleName(TeacherRoleName) compose
              addType
          )
          .transformForInsertFact(TeacherAnnouncementCols, AnnouncementEntity)
      )
      tempDF.unionOptionalByNameWithEmptyCheck(transformedRecipient)
    }

    transformed.map(DataSink(TeacherAnnouncementTransformedSink, _))
  }
}

object TeacherAnnouncementTransform {

  val TeacherAnnouncementTransformService = "transform-teacher-announcement"
  val ParquetTeacherAnnouncementSource = "parquet-teacher-announcement-source"
  val TeacherAnnouncementTransformedSink = "teacher-announcement-transformed-sink"

  val session = SparkSessionUtils.getSession(TeacherAnnouncementTransformService)
  val service = new SparkBatchService(TeacherAnnouncementTransformService, session)

  val TeacherAnnouncementCols = CommonAnnouncementCols ++ Map("teacherId" -> "fa_admin_id")

  val recipientColumnNames: Seq[String] = Seq(ClassIdsColName, StudentIdsColName)

  def main(args: Array[String]): Unit = {
    val transform = new TeacherAnnouncementTransform(session, service)
    service.run(transform.transform())
  }
}
