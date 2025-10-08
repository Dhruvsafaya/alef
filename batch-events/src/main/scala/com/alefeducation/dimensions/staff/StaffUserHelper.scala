package com.alefeducation.dimensions.staff

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.role.RoleTransform.RoleEntity
import com.alefeducation.models.RoleModel.RoleUuid
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources
import org.apache.spark.sql.functions.{col, explode_outer, lit, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object StaffUserHelper {

  val ExtraProps: List[(String, String)] = List(("mergeSchema", "true"))

  val RolesToExclude: List[String] = List("TEACHER", "STUDENT", "GUARDIAN")

  val RedshiftRoleDim = "redshift-dim-role-sink"

  val StaffUserEntity = "staff_user"
  val StaffUserSchoolRoleEntity = "susra"

  val StaffSourceName = "staff-source"
  val StaffDeletedSourceName = "staff-delete-source"

  val StaffUserMembershipName = "membershipsV3"

  val StaffUserMapping: Map[String, String] = Map(
    "eventType" -> s"${StaffUserEntity}_event_type",
    "uuid" -> s"${StaffUserEntity}_id",
    "onboarded" -> s"${StaffUserEntity}_onboarded",
    "expirable" -> s"${StaffUserEntity}_expirable",
    "avatar" -> s"${StaffUserEntity}_avatar",
    s"${StaffUserEntity}_status" -> s"${StaffUserEntity}_status",
    s"${StaffUserEntity}_active_until" -> s"${StaffUserEntity}_active_until",
    "excludeFromReport" -> s"${StaffUserEntity}_exclude_from_report",
    "enabled" -> s"${StaffUserEntity}_enabled",
    "occurredOn" -> "occurredOn"
  )

  val StaffUserIdKey = "dim_staff_user"
  val StaffUserSchoolRoleAssociationIdKey = "dim_staff_user_school_role_association"

  val StaffUserDwIdCol = "rel_staff_user_dw_id"

  val ActiveStatus = 1
  val InActiveStatus = 2

  def selectAdmin(df: DataFrame): Option[DataFrame] = {
    if (df.columns.contains(StaffUserMembershipName)) {
      Some(
        df.withColumn("mcols", explode_outer(col(StaffUserMembershipName)))
          .addColIfNotExists("role", StringType)
          .withColumn("role",
                      when(col("mcols.role").isNotNull, col("mcols.role"))
                        .when(col("role").isNotNull, col("role"))
                        .otherwise(lit("UNKNOWN")))
          .transform(filterByRole)
      )
    } else if (df.columns.contains("role")) {
      Some(filterByRole(df))
    } else {
      None
    }
  }

  def explodeMembership(df: DataFrame): DataFrame =
    df.withColumn("mcols", explode_outer(col(StaffUserMembershipName)))
      .addColIfNotExists("role", StringType)
      .withColumn("role",
        when(col("mcols.role").isNotNull, col("mcols.role"))
          .when(col("role").isNotNull, col("role"))
          .otherwise(lit("UNKNOWN")))

  def filterByRole(df: DataFrame): DataFrame = {
    df.filter(!col("role").isin(RolesToExclude: _*))
  }

  def readRoleFromRedshift(session: SparkSession, service: SparkBatchService): DataFrame = {
    import session.implicits._
    service.readFromRedshift[RoleUuid](RedshiftRoleDim)
      .filter(col(s"${RoleEntity}_status") === ActiveStatus)
      .selectOneByRowNumber(List(s"${RoleEntity}_name"), s"${RoleEntity}_created_time", asc = true)
  }

  def colsToMapping(cols: List[String]): Map[String, String] = cols.map(c => c -> c).toMap

  def setScdTypeIIUpdateForUser(dataFrame: DataFrame, name: String): Map[String, String] = {

    val targetTableDim = s"dim_$name"
    val targetTableRel = s"rel_$name"
    val idColRel = s"${name}_id"
    val idColDim = idColRel
    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()
    val stagingTable = s"staging_$targetTableDim"
    val insertCols = dataFrame.columns.mkString(", ")

    val updateStatusAndActiveUntil =
      s"""
       begin transaction;
       update $relSchema.$targetTableRel set ${name}_status = 2
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_status = 1 and target.${name}_active_until is null;

       update $relSchema.$targetTableRel set ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_status = 2
       from $dimSchema.$targetTableDim target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColDim
       where target.${name}_created_time <= staging.${name}_created_time and
       target.${name}_status = 1 and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColDim
       where target.${name}_created_time <= staging.${name}_created_time
       and target.${name}_active_until is null;

       insert into $relSchema.$targetTableRel ($insertCols)
       (select $insertCols from $dimSchema.$stagingTable);
       DROP table $dimSchema.$stagingTable;
       end transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }
}
