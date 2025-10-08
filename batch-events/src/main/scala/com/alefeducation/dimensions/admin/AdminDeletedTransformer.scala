package com.alefeducation.dimensions.admin

import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.admin.AdminHelper.setScdTypeIIUpdateForUser
import com.alefeducation.models.AdminUserModel.AdminUserDeletedDfCols
import com.alefeducation.util.Constants.UserDeletedEvent
import com.alefeducation.util.Helpers.{AdminUserEntity, DeltaAdminUserSink, ParquetUserDeletedSource, RedshiftAdminUserSink}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AdminDeletedTransformer(implicit val session: SparkSession) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transformAdminDeleted(adminDeleted: Option[DataFrame], cachedRedshift: DataFrame): List[Sink] = {
    val userDeletedTransformed = enrichWithCachedData(adminDeleted, cachedRedshift)
    val userDeleted = userDeletedTransformed.map(_.transformForSCD(AdminUserDeletedDfCols, AdminUserEntity, ids = List("admin_uuid")))

    val redshiftDeletedSink = userDeleted.map(_.toRedshiftSCDSink(RedshiftAdminUserSink, UserDeletedEvent, AdminUserEntity, setScdTypeIIUpdateForUser))
    val deltaDeletedSink = userDeleted.flatMap(
      _.transform(AdminHelper.transformRenameUUID(AdminUserEntity))
        .toSCD(uniqueIdColumns = List("admin_id"))
        .map(_.toSink(DeltaAdminUserSink, AdminUserEntity)))
    val deletedUserParquetSink = adminDeleted.map(_.toParquetSink(ParquetUserDeletedSource))

    (redshiftDeletedSink ++ deltaDeletedSink ++ deletedUserParquetSink).toList
  }

  private def enrichWithCachedData(df: Option[DataFrame], cachedDf: DataFrame): Option[DataFrame] =
    df.map(
      _.selectLatestByRowNumber(List("uuid"))
        .join(cachedDf, $"uuid" === $"admin_uuid")
        .drop($"admin_status")
    )

}
