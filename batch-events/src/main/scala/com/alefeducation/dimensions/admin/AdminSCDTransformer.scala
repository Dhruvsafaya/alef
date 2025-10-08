package com.alefeducation.dimensions.admin

import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.admin.AdminHelper.setScdTypeIIUpdateForUser
import com.alefeducation.models.AdminUserModel.AdminUserRedshiftCols
import com.alefeducation.util.Constants.{UserCreated, UserDisabled, UserEnabled, UserUpdateEnableDisable}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.{greatest, lit, when}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AdminSCDTransformer(implicit val session: SparkSession) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transformAdminSCD(adminUserSource: Option[DataFrame],
                        adminSchoolChangeSource: Option[DataFrame],
                        cachedDf: DataFrame): List[Sink] = {
    val adminUserUpdated = adminUserSource.flatMap(_.filter($"eventType" =!= UserCreated)
      .drop("roles", "permissions", "enabled")
      .withColumn("school", $"school.uuid").checkEmptyDf)

    val adminEnabledDisabled = adminUserUpdated.flatMap(_
      .filter($"eventType".isin(UserEnabled, UserDisabled)).checkEmptyDf
      .map(_
        .selectLatestByRowNumber(List("uuid"))
        .select($"uuid".as("status_uuid"), $"eventType".as("status_eventType"))
      )
    )

    val adminUpdatedWithEventType = adminUserUpdated.map(updated =>
      adminEnabledDisabled.map(enabledDisabled =>
        updated.join(enabledDisabled, $"uuid" === $"status_uuid", "left")
          .withColumn("eventTypeCalculated", when($"status_eventType".isNotNull, $"status_eventType")
            .otherwise($"eventType"))
          .select(updated("*"), $"eventTypeCalculated")
          .withColumn("eventType", $"eventTypeCalculated")
          .drop("eventTypeCalculated")
      ).getOrElse(updated)
    )

    val adminUpdates: Option[DataFrame] = adminUpdatedWithEventType.map(mutated => {
      val latest = mutated.selectLatestByRowNumber(List("uuid"))
      latest.join(cachedDf, $"uuid" === $"admin_uuid")
        //if enable disable event came take status from event, else from db
        .withColumn("admin_status",
          when($"eventType" === UserEnabled, lit(ActiveEnabled))
            .otherwise(
              when($"eventType" === UserDisabled, lit(Disabled))
                .otherwise($"admin_status")
            )
        )
        .withColumn("occurredOn", greatest($"occurredOn", $"admin_created_time".cast(StringType)))
        .select(
          $"uuid",
          $"school_uuid".as("school"),
          $"admin_status",
          $"avatar",
          $"onboarded",
          $"expirable",
          $"role",
          $"excludeFromReport",
          $"occurredOn"
        )
    }
    )

    val adminSchoolChangeSourcedScd = enrichWithCachedData(adminSchoolChangeSource, cachedDf)
    val scdTransformed = getTransformedDfForSCDProcess(adminUpdates, adminSchoolChangeSourcedScd).map(_
      .selectColumnsWithMapping(AdminUserRedshiftCols)
      .appendTimestampColumns(AdminUserEntity)
      .withColumn(s"${AdminUserEntity}_active_until", lit(NULL).cast(TimestampType))
    )
    val redshiftSCDSink = scdTransformed
      .map(_.toRedshiftSCDSink(RedshiftAdminUserSink, UserUpdateEnableDisable, AdminUserEntity, setScdTypeIIUpdateForUser))
    val deltaSCDSink = getDeltaSCDSink(scdTransformed)

    (redshiftSCDSink ++ deltaSCDSink).toList
  }



  private def getDeltaSCDSink(userSCDTransformed: Option[DataFrame]) = {
    userSCDTransformed.flatMap(
      _.transform(AdminHelper.transformRenameUUID(AdminUserEntity))
        .toSCD(uniqueIdColumns = List("admin_id"))
        .map(_.toSink(DeltaAdminUserSink, AdminUserEntity)))
  }

  private def enrichWithCachedData(df: Option[DataFrame], cachedDf: DataFrame): Option[DataFrame] = {
    val cacheCasted = cachedDf.withColumn("admin_created_time", $"admin_created_time".cast(StringType))
    df.map(
      _.selectLatestByRowNumber(List("uuid"))
        .join(cacheCasted, $"uuid" === $"admin_uuid" && $"occurredOn" > $"admin_created_time")
        .select(
          $"uuid",
          $"targetSchoolId".as("school"),
          $"admin_status".as("admin_status"),
          $"admin_avatar".as("avatar"),
          $"admin_onboarded".as("onboarded"),
          $"admin_expirable".as("expirable"),
          $"role_uuid".as("role"),
          $"admin_exclude_from_report".as("excludeFromReport"),
          $"occurredOn"
        )
    )
  }

  private def getTransformedDfForSCDProcess(
                                             adminUserMutatedSource: Option[DataFrame],
                                             adminSchoolChangeSource: Option[DataFrame]): Option[DataFrame] = {
    (adminUserMutatedSource, adminSchoolChangeSource) match {
      case (None, None) => None
      case (Some(df), None) => adminUserMutatedSource
      case (None, Some(df)) => adminSchoolChangeSource
      case (Some(df1), Some(df2)) =>
        val onlyUpdated = df1.join(df2, df1("uuid") === df2("uuid"), "leftanti").select(df1("*"))
        val onlySchoolMoved = df2.join(df1, df2("uuid") === df1("uuid"), "leftanti").select(df2("*"))
        val updatedWithSchoolMove = df1.as("df1").join(df2.as("df2"), usingColumn = "uuid")
          .withColumn("finalSchool", $"df2.school")
          .withColumn("finalOccurredOn", greatest($"df1.occurredOn".cast(StringType), $"df2.occurredOn".cast(StringType)))
          .select($"df1.*", $"finalSchool", $"finalOccurredOn")
          .withColumn("school", $"finalSchool")
          .withColumn("occurredOn", $"finalOccurredOn")
          .drop("finalOccurredOn", "finalSchool")

        val finalDf = onlyUpdated.unionByName(onlySchoolMoved).unionByName(updatedWithSchoolMove)
        Some(finalDf)
    }
  }

}
