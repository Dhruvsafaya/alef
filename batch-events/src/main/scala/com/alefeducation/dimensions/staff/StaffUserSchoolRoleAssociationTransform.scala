package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.staff.StaffUserHelper._
import com.alefeducation.dimensions.staff.StaffUserSchoolRoleAssociationTransform.{StaffSchoolMovedSourceName, StaffUserSchoolRoleAssociationCols, StaffUserSchoolRoleAssociationColumns}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{UserDisabled, UserEnabled}
import com.alefeducation.util.Resources.{getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, size, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class StaffUserSchoolRoleAssociationTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  val unionCols: List[String] = "eventType" :: "occurredOn" :: StaffUserSchoolRoleAssociationCols

  val uniqueKey: List[String] = List(
    s"${StaffUserSchoolRoleEntity}_staff_id"
  )

  val eventType: String = "eventType"
  val orderBy: String = "occurredOn"

  def transform(): Option[Sink] = {
    val staffSourceName = getNestedString(serviceName, StaffSourceName)
    val schoolMovedName = getNestedString(serviceName, StaffSchoolMovedSourceName)
    val sinkName = getSink(serviceName).head

    val staffDf = service.readOptional(staffSourceName, session, extraProps = ExtraProps)
      .map(_.filter(col(eventType) =!= UserEnabled && col(eventType) =!= UserDisabled))
    val staffSchoolMovedDf = service.readOptional(schoolMovedName, session, extraProps = ExtraProps)

    val (oldStaffDf, newStaffDf) = partition(staffDf)
    val (oldStaffMovedDf, newStaffMoveDf) = partition(staffSchoolMovedDf)

    val oldStaffWithColsDf = oldStaffDf.map(handleOldEvent)
    val oldStaffMovedWithColsDf = oldStaffMovedDf.map(handleOldEvent)
    val newStaffWithColsDf = newStaffDf.map(handleNewEvent)
    val newStaffMovedWithColsDf = newStaffMoveDf.map(handleNewEvent)

    val oldCombinedDf = combine(oldStaffWithColsDf, oldStaffMovedWithColsDf)
    val newCombinedDf = combine(newStaffWithColsDf, newStaffMovedWithColsDf)

    val startId = service.getStartIdUpdateStatus(StaffUserSchoolRoleAssociationIdKey)

    val transformed = combine(oldCombinedDf, newCombinedDf)
      .map(
        _.transformForSCDTypeII(
            StaffUserSchoolRoleAssociationColumns,
            StaffUserSchoolRoleEntity,
            uniqueKey,
            orderBy
        ).genDwId(s"${StaffUserSchoolRoleEntity}_dw_id", startId)
      )

    transformed.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> StaffUserSchoolRoleAssociationIdKey)))
  }

  def combine(staffDf: Option[DataFrame], staffMoveDf: Option[DataFrame]): Option[DataFrame] = {
    (for {
      staffDf <- staffDf
      staffMovedDf <- staffMoveDf
      df = staffDf.unionByName(staffMovedDf, allowMissingColumns = true)
    } yield df).orElse(staffDf).orElse(staffMoveDf)
  }

  def enrichRoleUuidFromRedshift(df: DataFrame): DataFrame = {
    df.join(readRoleFromRedshift(session, service), col(s"${StaffUserSchoolRoleEntity}_role_name") === col("role_name"), "left")
        .withColumn(s"${StaffUserSchoolRoleEntity}_role_uuid", col("role_uuid")
    )
  }

  def partition(df: Option[DataFrame]): (Option[DataFrame], Option[DataFrame]) = {
    if (df.exists(_.columns.contains(StaffUserMembershipName))) {
      val newEventsDf = df.map(_.filter(col(StaffUserMembershipName).isNotNull && size(col(StaffUserMembershipName)) > 0)).flatMap(_.checkEmptyDf)
      val oldEventsDf = df.map(_.filter(col(StaffUserMembershipName).isNull || size(col(StaffUserMembershipName)) === 0)).flatMap(_.checkEmptyDf)
      oldEventsDf -> newEventsDf
    } else {
      df -> None
    }
  }

  def handleNewEvent(df: DataFrame): DataFrame = {
    df.transform(selectCols _ compose addSchool compose addOrganization compose addRoleUuid compose
      addRoleName compose addStaffId compose filterByRole compose explodeMembership
    )
  }

  def handleOldEvent(df: DataFrame): DataFrame = {
    df.transform(selectCols _ compose enrichRoleUuidFromRedshift compose addStaffId compose addRoleName compose
      addOldSchool compose addNullOrganization compose filterByRole)
  }

  def selectCols(df: DataFrame): DataFrame = df.select(unionCols.map(col): _*)

  def addStaffId(df: DataFrame): DataFrame = df.withColumnRenamed("uuid", s"${StaffUserSchoolRoleEntity}_staff_id")
  def addSchool(df: DataFrame): DataFrame =
    df.withColumn(
      s"${StaffUserSchoolRoleEntity}_school_id",
      when(col("mcols.schoolId").isNotNull, col("mcols.schoolId"))
        .otherwise(lit(null).cast(StringType))
    )

  def addOldSchool(df: DataFrame): DataFrame =
    if (df.columns.contains("school")) {
      df.withColumn(s"${StaffUserSchoolRoleEntity}_school_id",
        when(col("school").isNotNull, col("school.uuid"))
        .otherwise(lit(null).cast(StringType)))
    } else if (df.columns.contains("targetSchoolId")) {
      df.withColumn(s"${StaffUserSchoolRoleEntity}_school_id",
        when(col("targetSchoolId").isNotNull, col("targetSchoolId"))
          .otherwise(lit(null).cast(StringType)))
    } else {
      df.withColumn(s"${StaffUserSchoolRoleEntity}_school_id", lit(null).cast(StringType))
    }

  def addOrganization(df: DataFrame): DataFrame =
    df.withColumn(
      s"${StaffUserSchoolRoleEntity}_organization",
      when(col("mcols.organization").isNotNull, col("mcols.organization"))
        .otherwise(lit(null).cast(StringType))
    )

  def addNullOrganization(df: DataFrame): DataFrame =
    df.withColumn(s"${StaffUserSchoolRoleEntity}_organization", lit(null).cast(StringType))

  def addRoleUuid(df: DataFrame): DataFrame =
    df.withColumn(
      s"${StaffUserSchoolRoleEntity}_role_uuid",
      when(col("mcols.roleId").isNotNull, col("mcols.roleId"))
        .otherwise(lit(null).cast(StringType))
    )

  def addRoleName(df: DataFrame): DataFrame =
    df.withColumnRenamed("role", s"${StaffUserSchoolRoleEntity}_role_name")
}

object StaffUserSchoolRoleAssociationTransform {

  val StaffUserSchoolRoleAssociationTransformService = "staff-user-school-role-association-transform"

  val StaffSchoolMovedSourceName = "school-moved-source"

  val StaffUserSchoolRoleAssociationCols: List[String] = List(
    s"${StaffUserSchoolRoleEntity}_staff_id",
    s"${StaffUserSchoolRoleEntity}_school_id",
    s"${StaffUserSchoolRoleEntity}_role_uuid",
    s"${StaffUserSchoolRoleEntity}_role_name",
    s"${StaffUserSchoolRoleEntity}_organization"
  )

  val StaffUserSchoolRoleAssociationColumns: Map[String, String] =
    colsToMapping(StaffUserSchoolRoleAssociationCols) +
      (s"${StaffUserSchoolRoleEntity}_status" -> s"${StaffUserSchoolRoleEntity}_status") +
      (s"${StaffUserSchoolRoleEntity}_active_until" -> s"${StaffUserSchoolRoleEntity}_active_until") +
      ("eventType" -> s"${StaffUserSchoolRoleEntity}_event_type") +
      ("occurredOn" -> "occurredOn")

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(StaffUserSchoolRoleAssociationTransformService)
    val service = new SparkBatchService(StaffUserSchoolRoleAssociationTransformService, session)

    val transformer = new StaffUserSchoolRoleAssociationTransform(session, service, StaffUserSchoolRoleAssociationTransformService)
    service.run(transformer.transform())
  }
}
