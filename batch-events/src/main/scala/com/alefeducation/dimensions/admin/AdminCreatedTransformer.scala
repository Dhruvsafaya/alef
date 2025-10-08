package com.alefeducation.dimensions.admin

import com.alefeducation.bigdata.Sink
import com.alefeducation.models.AdminUserModel.AdminUserRedshiftCols
import com.alefeducation.util.Constants.UserCreated
import com.alefeducation.util.Helpers.{AdminUserEntity, DeltaAdminUserSink, RedshiftAdminUserSink, RedshiftUserSink}
import com.alefeducation.util.redshift.options.UpsertOptions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class AdminCreatedTransformer(implicit val session: SparkSession) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transformAdminCreated(adminUserSource: Option[DataFrame], adminsNotCreated: Option[DataFrame]): Option[DataFrame] = {
    val adminUserCreated = adminUserSource.flatMap(_.filter($"eventType" === UserCreated).checkEmptyDf)

    val userCreatedDf = adminUserCreated.unionOptionalByName(adminsNotCreated)
      .map(
        _.withColumn("school", $"school.uuid")
          .transformForSCD(AdminUserRedshiftCols, AdminUserEntity, ids = List("uuid"))
      )
    userCreatedDf
  }

  def getAdminCreatedSinks(userCreatedTransformed: Option[DataFrame],
                           adminsNotCreated: Option[DataFrame]): List[Sink] = {

    val relUser = userCreatedTransformed
      .map(_.select(
        col("admin_uuid").as("user_id"),
        col("admin_created_time").as("occurredOn").cast("string"))
      )

    //To handle the case when update event arrives without created event
    val adminsNotCreatedIds = adminsNotCreated
      .map(_.select(
        col("uuid").as("user_id"),
        col("occurredOn"))
      )
    val allRelUsers = relUser.unionOptionalByName(adminsNotCreatedIds)
    val allRelUserSinks = allRelUsers.map(
      _.transformForInsertDwIdMapping("user", "ADMIN")
        .toRedshiftUpsertSink(
          RedshiftUserSink,
          UpsertOptions.RelUser.targetTableName,
          matchConditions = UpsertOptions.RelUser.matchConditions("ADMIN"),
          columnsToUpdate = UpsertOptions.RelUser.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelUser.columnsToInsert,
          isStaging = true
        )
    )

    val deltaSink =
      userCreatedTransformed.flatMap(_.transform(AdminHelper.transformRenameUUID(AdminUserEntity)).toCreate().map(_.toSink(DeltaAdminUserSink)))
    val redshiftSink = userCreatedTransformed.map(_.toRedshiftInsertSink(RedshiftAdminUserSink, UserCreated))
    (allRelUserSinks ++ deltaSink ++ redshiftSink).toList
  }

  /**
   * TODO: When Issue is fixed from app, consider removing this logic
   * To handle the case when update event arrives without created event
   * Checks in rel_user if user_id does not exists.
   * Returns Dataframe with user_ids to be inserted in rel_user
   */
  def getAdminsNotCreatedYet(adminUserUpdated: Option[DataFrame], redshiftRelUser: DataFrame): Option[DataFrame] = {
    //To handle the case when update event arrives without created event
    val adminLatestUpdatedDf = adminUserUpdated.map(_.selectLatestByRowNumber(List("uuid")))
    //TODO: Check if full rel_user table scan can be avoided
    val adminsToBeCreated = adminLatestUpdatedDf.flatMap(_
      .join(redshiftRelUser, col("uuid") === col("user_id"), "leftanti")
      .drop("user_dw_id", "user_id")
      .checkEmptyDf
    )
    adminsToBeCreated
  }

}
