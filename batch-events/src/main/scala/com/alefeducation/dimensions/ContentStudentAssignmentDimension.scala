package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.{addColsForIWH, getParquetSinks, getRedshiftSinks}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ContentStudentAssignmentDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  private val ASSIGNED = 1
  private val UNASSIGNED = 0
  private val orderByField = "occurredOn"
  private val uniqueKey = List("contentId", "studentId", "classId", "mloId", "contentType")
  private val uniqueIdStudentContentColumns = List(
    "content_student_association_step_id",
    "content_student_association_student_id",
    "content_student_association_class_id",
    "content_student_association_lo_id",
    "content_student_association_type",
    "content_student_association_content_type"
  )
  private val uniqueIdClassContentColumns = List(
    "content_student_association_step_id",
    "content_student_association_class_id",
    "content_student_association_lo_id",
    "content_student_association_type",
    "content_student_association_content_type"
  )

  val deltaClassContentMatchConditions: String =
    s"""
       |${Alias.Delta}.${ContentStudentAssociationEntity}_step_id = ${Alias.Events}.${ContentStudentAssociationEntity}_step_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_class_id = ${Alias.Events}.${ContentStudentAssociationEntity}_class_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_lo_id = ${Alias.Events}.${ContentStudentAssociationEntity}_lo_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_type = ${Alias.Events}.${ContentStudentAssociationEntity}_type
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_content_type = ${Alias.Events}.${ContentStudentAssociationEntity}_content_type
     """.stripMargin

  val deltaStudentContentMatchConditions: String =
    s"""
       |${Alias.Delta}.${ContentStudentAssociationEntity}_step_id = ${Alias.Events}.${ContentStudentAssociationEntity}_step_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_student_id = ${Alias.Events}.${ContentStudentAssociationEntity}_student_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_class_id = ${Alias.Events}.${ContentStudentAssociationEntity}_class_id
       | and
       |${Alias.Delta}.${ContentStudentAssociationEntity}_lo_id = ${Alias.Events}.${ContentStudentAssociationEntity}_lo_id
       | and
       | ${Alias.Delta}.${ContentStudentAssociationEntity}_type = ${Alias.Events}.${ContentStudentAssociationEntity}_type
       | and
       | ${Alias.Delta}.${ContentStudentAssociationEntity}_content_type = ${Alias.Events}.${ContentStudentAssociationEntity}_content_type
     """.stripMargin

  override def transform(): List[Sink] = {
    val studentContentAssignmentToggleDf = read(ParquetStudentContentSource, session)
    val classAssocDf = read(ParquetContentClassSource, session)

    val parquetStudentContentAssignmentToggleSinks = getParquetSinks(session, ParquetStudentContentSource, studentContentAssignmentToggleDf)
    val parquetClassContentAssignmentToggleSinks = getParquetSinks(session, ParquetContentClassSource, classAssocDf)
    val parquetSinks = parquetStudentContentAssignmentToggleSinks ++ parquetClassContentAssignmentToggleSinks

    val transformedStudentContentAssignmentToggleDf =
      studentContentAssignmentToggleDf
        .transformIfNotEmpty(transformStatusesAndAssociationType)

    val transformedClassContentAssignmentToggleDf =
      classAssocDf.withColumn("studentId", lit(null).cast(StringType)).transformIfNotEmpty(transformStatusesAndAssociationType)

    val studentContentAssignmentSinks: List[Sink] =
      getSinks(transformedStudentContentAssignmentToggleDf, StudentContentAssignedEvent, RedshiftStudentContentAssignmentSink, uniqueIdStudentContentColumns)
    val studentContentUnassignedSinks: List[Sink] =
      getSinks(transformedStudentContentAssignmentToggleDf, StudentContentUnAssignedEvent, RedshiftStudentContentUnassignmentSink, uniqueIdStudentContentColumns)

    val classContentAssignmentSinks: List[Sink] =
      getSinks(transformedClassContentAssignmentToggleDf, ClassContentAssignedEvent, RedshiftClassContentAssignmentSink, uniqueIdClassContentColumns)
    val classContentUnassignedSinks: List[Sink] =
      getSinks(transformedClassContentAssignmentToggleDf, ClassContentUnAssignedEvent, RedshiftClassContentUnAssignmentSink, uniqueIdClassContentColumns)

    val redshiftSinks = studentContentAssignmentSinks ++ classContentAssignmentSinks ++ studentContentUnassignedSinks ++ classContentUnassignedSinks
    val deltaStudentContentSinks = createDeltaSinks(transformedStudentContentAssignmentToggleDf,
                                                    deltaStudentContentMatchConditions,
                                                    uniqueIdStudentContentColumns,
                                                    DeltaContentStudentAssignmentSink)
    val deltaClassContentSinks =
      createDeltaSinks(transformedClassContentAssignmentToggleDf,
                       deltaClassContentMatchConditions,
                       uniqueIdClassContentColumns,
                       DeltaContentClassAssignmentSink)
    val deltaSinks = deltaStudentContentSinks ++ deltaClassContentSinks

    redshiftSinks ++ deltaSinks ++ parquetSinks
  }

  def createDeltaSinks(df: DataFrame, deltaMatchConditions: String, uniqueColumns: List[String], sinkName: String): List[Sink] = {
    val dataFrameWithTimeColumns = df.transformIfNotEmpty(addColsForIWH(ContentStudentDimensionCols, ContentStudentAssociationEntity))

    dataFrameWithTimeColumns
      .toIWH(deltaMatchConditions, uniqueColumns)
      .map(_.toSink(sinkName, ContentStudentAssociationEntity))
      .toList
  }

  def getSinks(df: DataFrame, eventName: String, redshiftSink: String, latestByIds: List[String])(implicit session: SparkSession): List[DataSink] = {
    if (df.isEmpty)
      return Nil

    getRedshiftSinks(
      session,
      df,
      List(eventName),
      ContentStudentDimensionCols,
      ContentStudentAssociationEntity,
      redshiftSink,
      latestByIds = latestByIds
    )
  }

  def transformStatusesAndAssociationType(df: DataFrame): DataFrame =
    (addStatusCol(ContentStudentAssociationEntity, orderByField, uniqueKey) _ andThen
      addUpdatedTimeCols(ContentStudentAssociationEntity, orderByField, uniqueKey) andThen
      transformAssociationAttachStatus)(df)

  def transformAssociationAttachStatus(df: DataFrame): DataFrame =
    df.withColumn("content_student_association_type",
                  when(col("eventType") isin (List(ClassContentAssignedEvent, ClassContentUnAssignedEvent): _*), 1).otherwise(2))
      .withColumn(
        "content_student_association_assign_status",
        when(col("eventType") isin (List(ClassContentAssignedEvent, StudentContentAssignedEvent): _*), ASSIGNED).otherwise(UNASSIGNED)
      )

}

object ContentStudentAssignmentDimension {
  def main(args: Array[String]): Unit =
    new ContentStudentAssignmentDimension(ContentStudentAssignmentDimensionName,
                                          SparkSessionUtils.getSession(ContentStudentAssignmentDimensionName)).run
}
