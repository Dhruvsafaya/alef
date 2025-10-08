package com.alefeducation.facts.announcement.principal

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

class PrincipalAnnouncementTransform(val session: SparkSession, val service: SparkBatchService) {

  import PrincipalAnnouncementTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val principalDf: Option[DataFrame] = service.readOptional(ParquetPrincipalAnnouncementSource, session)

    val transformedPrincipal = principalDf.map(_.transform(transformColumnIntoArray(SchoolIdColName)))

    val accumulator: Option[DataFrame] = None
    val transformed = recipientColumnNames.foldLeft(accumulator) { (tempDF, recipientColName) =>
      val transformedRecipient = transformedPrincipal.map(
        _.filter(col(recipientColName).isNotNull)
          .transform(
            explodeRecipients(recipientColName) _ compose
              createRecipientTypeId(recipientColName) compose
              createRecipientTypeDesc(recipientColName) compose
              addStatus(ActiveStatus) compose
              addRoleName(PrincipalRoleName) compose
              addType
          )
          .transformForInsertFact(PrincipalAnnouncementCols, AnnouncementEntity)
      )
      tempDF.unionOptionalByNameWithEmptyCheck(transformedRecipient)
    }

    transformed.map(DataSink(PrincipalAnnouncementTransformedSink, _))
  }
}

object PrincipalAnnouncementTransform {

  val PrincipalAnnouncementTransformService = "transform-principal-announcement"
  val ParquetPrincipalAnnouncementSource = "parquet-principal-announcement-source"
  val PrincipalAnnouncementTransformedSink = "principal-announcement-transformed-sink"

  val session = SparkSessionUtils.getSession(PrincipalAnnouncementTransformService)
  val service = new SparkBatchService(PrincipalAnnouncementTransformService, session)

  val PrincipalAnnouncementCols = CommonAnnouncementCols ++ Map("principalId" -> "fa_admin_id")

  val recipientColumnNames: Seq[String] = Seq(SchoolIdColName, GradeIdsColName, ClassIdsColName, StudentIdsColName)

  def main(args: Array[String]): Unit = {
    val transform = new PrincipalAnnouncementTransform(session, service)
    service.run(transform.transform())
  }
}
