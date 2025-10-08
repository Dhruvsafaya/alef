package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.models.GuardianModel.{DimGuardian, RelGuardian}
import com.alefeducation.models.StudentModel.StudentDim
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable.Map

object GuardianCommonUtils {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._

  val GuardianActiveUntilColName: String = "guardian_active_until"
  val GuardianCreatedTimeColName: String = "guardian_created_time"
  val GuardianStatusColName: String = "guardian_status"
  val GuardianInvitationStatusColName: String = "guardian_invitation_status"
  val GuardianIdColName = "guardian_id"
  val StudentIdColName = "student_id"
  val StudentActiveUntilColName = "student_active_until"

  def transformGuardianData(guardianFromApp: Dataset[Row]): DataFrame = {
    val dataFrame = guardianFromApp.withColumn("eventType", lit("Created"))
    dataFrame.transformForSCD(GuardianDimensionCols, GuardianEntity)
  }

  def createDeltaSink(dataFrame: DataFrame, sinkName: String, uniqueId: String): DeltaSCDSink = {
    val deltaMatchConditions = s"${Alias.Delta}.$uniqueId = ${Alias.Events}.$uniqueId"
    dataFrame
      .orderBy(desc("guardian_created_time"))
      .coalesce(1)
      .withColumnRenamed("student_id", "guardian_student_id")
      .toSCDContext(
        matchConditions = deltaMatchConditions,
        uniqueIdColumns = List(uniqueId)
      )
      .toSink(sinkName, GuardianEntity)
  }

  def getRedshiftGuardians(session: SparkSession, service: SparkBatchService): DataFrame = {
    import session.implicits._

    val relGuardian = service
      .readFromRedshift[RelGuardian](RedshiftGuardianRelSink)
      .transform(relGuardianToCommonSchema)

    val dimGuardian = service
      .readFromRedshift[DimGuardian](RedshiftGuardianDimSource)
      .transform(dimGuardianToCommonSchema(_, session, service))

    relGuardian.unionByName(dimGuardian).cache()
  }

  private def relGuardianToCommonSchema(relGuardian: DataFrame): DataFrame = {
    relGuardian
      .filter(col(GuardianActiveUntilColName).isNull)
      .select(
        col(GuardianCreatedTimeColName).as("occurredOn"),
        col(GuardianStatusColName).as("status"),
        col(GuardianIdColName).as("guardianId"),
        col(StudentIdColName).as("studentId"),
        col(GuardianInvitationStatusColName).as("invitationStatus")
      )
      .unpersist()
  }

  private def dimGuardianToCommonSchema(dimGuardian: DataFrame, session: SparkSession, service: SparkBatchService): DataFrame = {
    import session.implicits._

    val guardian = dimGuardian.filter(col(GuardianActiveUntilColName).isNull)

    val student = service
      .readFromRedshift[StudentDim](RedshiftDimStudentSink)
      .filter(col("student_active_until").isNull)

    student
      .join(guardian, $"guardian_student_dw_id" === $"student_dw_id", "inner")
      .select(
        col(GuardianCreatedTimeColName).as("occurredOn"),
        col(GuardianStatusColName).as("status"),
        col(GuardianIdColName).as("guardianId"),
        col(StudentIdColName).as("studentId"),
        col(GuardianInvitationStatusColName).as("invitationStatus")
      )
  }

  def scdUpdateOption(dataFrame: DataFrame, name: String): Map[String, String] = {
    val targetTableDim = s"dim_$name"
    val targetTableRel = s"rel_$name"
    val idColRel = StudentIdColName
    val idColDim = "guardian_student_dw_id"
    val dimSchema = Resources.redshiftSchema()
    val relSchema = Resources.redshiftStageSchema()
    val stagingTable = s"staging_$targetTableDim"
    val insertCols = dataFrame.columns.mkString(", ")
    val updateStatusAndActiveUntil =
      s"""
       begin transaction;
       create table $dimSchema.${stagingTable}_temp as
       SELECT * FROM (SELECT *, row_number() over (partition by guardian_id order by guardian_created_time)
       as rnk FROM $dimSchema.$stagingTable)a WHERE a.rnk = 1;

       update $relSchema.$targetTableRel set ${name}_status = 2, ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.${stagingTable}_temp staging on staging.guardian_id = target.guardian_id
       where target.${name}_status = 1 and target.${name}_active_until is null and target.student_id is null;

       update $relSchema.$targetTableRel set ${name}_status = 2, ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_status = 1 and target.${name}_active_until is null;

       update $relSchema.$targetTableRel set ${name}_active_until = staging.${name}_created_time
       from $relSchema.$targetTableRel target
       join $dimSchema.$stagingTable staging on staging.$idColRel = target.$idColRel
       where target.${name}_status = 4 and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_status = 2, ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $dimSchema.${stagingTable}_temp staging on staging.guardian_id = target.guardian_id
       where target.${name}_status = 1 and target.${name}_active_until is null and target.${name}_student_dw_id is null;

       update $dimSchema.$targetTableDim set ${name}_status = 2, ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $relSchema.rel_user user1 on user1.user_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on user1.user_id = staging.$idColRel
       where target.${name}_status = 1 and target.${name}_active_until is null;

       update $dimSchema.$targetTableDim set ${name}_active_until = staging.${name}_created_time
       from $dimSchema.$targetTableDim target
       join $relSchema.rel_user user1 on user1.user_dw_id = target.$idColDim
       join $dimSchema.$stagingTable staging on user1.user_id = staging.$idColRel
       where target.${name}_status = 4 and target.${name}_active_until is null;

       insert into $relSchema.$targetTableRel ($insertCols)
       (select $insertCols from $dimSchema.$stagingTable);
       DROP table $dimSchema.${stagingTable}_temp;
       DROP table $dimSchema.$stagingTable;
       end transaction
     """.stripMargin
    Map("dbtable" -> s"$dimSchema.$stagingTable", "postactions" -> updateStatusAndActiveUntil)
  }

}
