package com.alefeducation.facts.announcement

import org.apache.spark.sql.functions.{array, coalesce, col, explode, lit, typedLit, when}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame}

object AnnouncementUtils {

  val RedshiftAnnouncementSink = "redshift-announcement-sink"
  val DeltaAnnouncementSink = "delta-announcement-sink"
  val DeltaAnnouncementSource = DeltaAnnouncementSink
  val AnnouncementEntity = "fa"

  val ActiveStatus = 1
  val DeletedStatus = 4

  val SchoolIdColName = "schoolId"
  val SchoolIdsColName = "schoolIds"
  val GradeIdsColName = "gradeIds"
  val ClassIdsColName = "classIds"
  val StudentIdsColName = "studentIds"

  val SchoolTypeId = 1
  val GradeTypeId = 2
  val ClassTypeId = 3
  val StudentTypeId = 4
  val UnknownTypeId = -1

  val RecipientColNameToTypeId: Column = typedLit(
    Map(
      SchoolIdColName -> SchoolTypeId,
      SchoolIdsColName -> SchoolTypeId,
      GradeIdsColName -> GradeTypeId,
      ClassIdsColName -> ClassTypeId,
      StudentIdsColName -> StudentTypeId,
    ))

  val SchoolTypeDesc = "SCHOOL"
  val GradeTypeDesc = "GRADE"
  val ClassTypeDesc = "CLASS"
  val StudentTypeDesc = "STUDENT"
  val UnknownTypeDesc = "UNKNOWN"

  val RecipientColNameToDescription: Column = typedLit(
    Map(
      SchoolIdColName -> SchoolTypeDesc,
      SchoolIdsColName -> SchoolTypeDesc,
      GradeIdsColName -> GradeTypeDesc,
      ClassIdsColName -> ClassTypeDesc,
      StudentIdsColName -> StudentTypeDesc,
    ))

  val PrincipalRoleName = "PRINCIPAL"
  val TeacherRoleName = "TEACHER"
  val SuperintendentRoleName = "SUPERINTENDENT"

  val AnnouncementTypeCol = "announcementType"

  val StudentsAnnType = "STUDENTS"
  val StudentsAnnTypeVal = 1
  val GuardiansAnnType = "GUARDIANS"
  val GuardiansAnnTypeVal = 2
  val GuardiansAndStudentsAnnType = "GUARDIANS_AND_STUDENTS"
  val GuardiansAndStudentsAnnTypeVal = 3

  val CommonAnnouncementCols = Map(
    "tenantId" -> "fa_tenant_id",
    "announcementId" -> "fa_id",
    "recipientId" -> "fa_recipient_id",
    "hasAttachment" -> "fa_has_attachment",
    "fa_status" -> "fa_status",
    "recipientType" -> "fa_recipient_type",
    "recipientTypeDesc" -> "fa_recipient_type_description",
    "roleName" -> "fa_role_id",
    "occurredOn" -> "occurredOn",
    "fa_type" -> "fa_type"
  )

  def createRecipientTypeDesc(recipientColName: String)(df: DataFrame): DataFrame = {
    createRecipientTypeColumn[String]("recipientTypeDesc", recipientColName, RecipientColNameToDescription, UnknownTypeDesc)(df)
  }

  def createRecipientTypeId(recipientColName: String)(df: DataFrame): DataFrame = {
    createRecipientTypeColumn[Int]("recipientType", recipientColName, RecipientColNameToTypeId, UnknownTypeId)(df)
  }

  private def createRecipientTypeColumn[A](columnName: String, recipientColName: String, recipientToType: Column, unknownType: A)(
      df: DataFrame): DataFrame = {
    df.withColumn(columnName, coalesce(recipientToType(recipientColName), lit(unknownType)))
  }

  def transformColumnIntoArray(columnName: String)(df: DataFrame): DataFrame = {
    df.withColumn(
      columnName,
      when(col(columnName).isNotNull, array(columnName)).otherwise(lit(null).cast(ArrayType(StringType)))
    )
  }

  def explodeRecipients(recipientColName: String)(df: DataFrame): DataFrame = {
    df.withColumn("recipientId", explode(col(recipientColName)))
  }

  def addStatus(status: Int)(df: DataFrame): DataFrame = {
    df.withColumn("fa_status", lit(status))
  }

  def addRoleName(name: String)(df: DataFrame): DataFrame = {
    df.withColumn("roleName", lit(name))
  }

  def addType(df: DataFrame): DataFrame = {
    df.withColumn("fa_type",
      when(col(AnnouncementTypeCol) === StudentsAnnType, StudentsAnnTypeVal)
      .when(col(AnnouncementTypeCol) === GuardiansAnnType, GuardiansAnnTypeVal)
        .when(col(AnnouncementTypeCol) === GuardiansAndStudentsAnnType, GuardiansAndStudentsAnnTypeVal)
        .otherwise(lit(null).cast(IntegerType))
    )
  }
}
