package com.alefeducation.dimensions.admin

import com.alefeducation.util.Resources
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

object AdminHelper {

  def transformRenameUUID(prefix: String)(df: DataFrame): DataFrame =
    df.withColumnRenamed("school_uuid", s"${prefix}_school_id")
      .withColumnRenamed(s"${prefix}_uuid", s"${prefix}_id")
      .withColumnRenamed("role_uuid", s"${prefix}_role_id")

  def getAllIncomingIds(adminUserSource: Option[DataFrame],
                        adminSchoolChangeSource: Option[DataFrame],
                        deletedAdmin: Option[DataFrame],
                        emptyDf: DataFrame
                       ) = {
    val mutatedEventUUIDS = adminUserSource.map(_.select("uuid")).getOrElse(emptyDf)
    val schoolMovedUUIDS = adminSchoolChangeSource.map(_.select("uuid")).getOrElse(emptyDf)
    val deletedDfUUIDS = deletedAdmin.map(_.select("uuid")).getOrElse(emptyDf)
    val requiredUUIDS = mutatedEventUUIDS.union(schoolMovedUUIDS).union(deletedDfUUIDS).distinct()
    requiredUUIDS
  }

  def setScdTypeIIUpdateForUser(dataFrame: DataFrame, name: String): Map[String, String] = {

    val targetTableDim = s"dim_$name"
    val targetTableRel = s"rel_$name"
    val idColRel = s"${name}_uuid"
    val idColDim = s"${name}_id"
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
       commit transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }

}
