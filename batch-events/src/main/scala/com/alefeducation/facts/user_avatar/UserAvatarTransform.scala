package com.alefeducation.facts.user_avatar

import com.alefeducation.base.SparkBatchService
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class UserAvatarTransform(val session: SparkSession, val service: SparkBatchService) {

  import UserAvatarTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(sourceName: String, sinkName: String): Option[DataSink] = {
    val startId = service.getStartIdUpdateStatus(UserAvatarKey)
    val userAvatarDf = service.readOptional(sourceName, session)

    val transformed = userAvatarDf.map(
      _.transformForInsertFact(UserAvatarCols, UserAvatarEntity)
        .genDwId(s"${UserAvatarEntity}_dw_id", startId)
    )

    transformed.map(df => DataSink(sinkName, df, controlTableUpdateOptions = Map(ProductMaxIdType -> UserAvatarKey)))
  }
}

object UserAvatarTransform {

  val UserAvatarService = "transform-user-avatar"
  val UserAvatarEntity = "fua"
  val UserAvatarKey = "fact_user_avatar"

  val UserAvatarCols = Map(
    "uuid" -> s"${UserAvatarEntity}_user_id",
    "avatarId" -> s"${UserAvatarEntity}_id",
    "tenantId" -> s"${UserAvatarEntity}_tenant_id",
    "gradeId" -> s"${UserAvatarEntity}_grade_id",
    "schoolId" -> s"${UserAvatarEntity}_school_id",
    "avatarType" -> s"${UserAvatarEntity}_avatar_type",
    "avatarFileId" -> s"${UserAvatarEntity}_avatar_file_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val session = SparkSessionUtils.getSession(UserAvatarService)
    val service = new SparkBatchService(UserAvatarService, session)

    val sourceName = getSource(UserAvatarService).head
    val sinkName = getSink(UserAvatarService).head

    val transformer = new UserAvatarTransform(session, service)
    service.run(transformer.transform(sourceName, sinkName))
  }
}
