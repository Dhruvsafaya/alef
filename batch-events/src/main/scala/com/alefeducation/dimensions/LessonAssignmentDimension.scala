package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.addColsForIWH
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class LessonAssignmentDimension(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import LessonAssignmentDimension._
  import com.alefeducation.util.BatchTransformerUtility._

  private val uniqueKey = List("studentId", "classId", "mloId")
  private val uniqueRedshiftIdStudentLessonColumns = List(
    "student_uuid",
    "class_uuid",
    "lo_uuid",
    "lesson_assignment_type"
  )
  private val uniqueRedshiftIdClassLessonColumns = List(
    "class_uuid",
    "lo_uuid",
    "lesson_assignment_type"
  )
  private val uniqueIdStudentLessonColumns = List(
    "lesson_assignment_student_id",
    "lesson_assignment_class_id",
    "lesson_assignment_lo_id",
    "lesson_assignment_type"
  )
  private val uniqueIdClassLessonColumns = List(
    "lesson_assignment_class_id",
    "lesson_assignment_lo_id",
    "lesson_assignment_type"
  )

  val deltaClassLessonMatchConditions: String =
    s"""
       |${Alias.Delta}.${LessonAssignmentEntity}_class_id = ${Alias.Events}.${LessonAssignmentEntity}_class_id
       | and
       |${Alias.Delta}.${LessonAssignmentEntity}_lo_id = ${Alias.Events}.${LessonAssignmentEntity}_lo_id
       | and
       |${Alias.Delta}.${LessonAssignmentEntity}_type = ${Alias.Events}.${LessonAssignmentEntity}_type
     """.stripMargin

  val deltaStudentLessonMatchConditions: String =
    s"""
       |${Alias.Delta}.${LessonAssignmentEntity}_student_id = ${Alias.Events}.${LessonAssignmentEntity}_student_id
       | and
       |${Alias.Delta}.${LessonAssignmentEntity}_class_id = ${Alias.Events}.${LessonAssignmentEntity}_class_id
       | and
       |${Alias.Delta}.${LessonAssignmentEntity}_lo_id = ${Alias.Events}.${LessonAssignmentEntity}_lo_id
       | and
       | ${Alias.Delta}.${LessonAssignmentEntity}_type = ${Alias.Events}.${LessonAssignmentEntity}_type
     """.stripMargin

  override def transform(): List[Sink] = {
    val classAssocDf = readOptional(ParquetLessonClassSource, session)
    val studentLessonAssignmentToggleDf = readOptional(ParquetLessonStudentSource, session)

    val transformedClassLessonAssignmentToggleDf =
      classAssocDf
        .map(
          _.withColumn("studentId", lit(null).cast(StringType))
            .transformForIWH2(
              LessonAssignmentDimensionCols,
              LessonAssignmentEntity,
              ClassTypeAssociation,
              List(ClassLessonAssignedEvent),
              List(ClassLessonUnAssignedEvent),
              uniqueKey
            ))

    val transformedStudentLessonAssignmentToggleDf =
      studentLessonAssignmentToggleDf
        .map(
          _.transformForIWH2(
            LessonAssignmentDimensionCols,
            LessonAssignmentEntity,
            StudentTypeAssociation,
            List(LearnerLessonAssignedEvent),
            List(LearnerLessonUnAssignedEvent),
            uniqueKey
          ))

    //Parquet Sinks
    val parquetClassLessonAssignmentToggleSinks = classAssocDf.map(_.toParquetSink(ParquetLessonClassSource))
    val parquetStudentLessonAssignmentToggleSinks = studentLessonAssignmentToggleDf.map(_.toParquetSink(ParquetLessonStudentSource))

    val parquetSinks = parquetStudentLessonAssignmentToggleSinks ++ parquetClassLessonAssignmentToggleSinks

    //Redshift Sinks
    val classLessonAssignmentSinks: Option[DataSink] = transformedClassLessonAssignmentToggleDf.map(
      _.toRedshiftIWHSink(RedshiftClassLessonAssignmentSink,
                          LessonAssignmentEntity,
                          ClassLessonAssignedEvent,
                          uniqueRedshiftIdClassLessonColumns,
                          isStagingSink = true))

    val studentLessonAssignmentSinks: Option[DataSink] = transformedStudentLessonAssignmentToggleDf.map(
      _.toRedshiftIWHSink(RedshiftStudentLessonAssignmentSink,
                          LessonAssignmentEntity,
                          LearnerLessonAssignedEvent,
                          uniqueRedshiftIdStudentLessonColumns,
                          isStagingSink = true))

    val redshiftSinks = (studentLessonAssignmentSinks ++ classLessonAssignmentSinks).toList

    //Delta Sinks
    val deltaClassLessonSinks: Option[Sink] = transformedClassLessonAssignmentToggleDf.flatMap(
      _.transform(transformRenameUUID)
        .toIWH(deltaClassLessonMatchConditions, uniqueIdClassLessonColumns)
        .map(_.toSink(DeltaLessonClassAssignmentSink, LessonAssignmentEntity)))

    val deltaStudentLessonSinks: Option[Sink] = transformedStudentLessonAssignmentToggleDf.flatMap(
      _.transform(transformRenameUUID)
        .toIWH(deltaStudentLessonMatchConditions, uniqueIdStudentLessonColumns)
        .map(_.toSink(DeltaLessonStudentAssignmentSink, LessonAssignmentEntity)))

    val deltaSinks = (deltaStudentLessonSinks ++ deltaClassLessonSinks).toList

    redshiftSinks ++ parquetSinks ++ deltaSinks
  }

  def createDeltaSinks(df: DataFrame, deltaMatchConditions: String, uniqueColumns: List[String], sinkName: String): List[Sink] = {
    val dataFrameWithTimeColumns = df.transformIfNotEmpty(addColsForIWH(LessonAssignmentDimensionCols, LessonAssignmentEntity))

    dataFrameWithTimeColumns
      .toIWH(deltaMatchConditions, uniqueColumns)
      .map(_.toSink(sinkName, LessonAssignmentEntity))
      .toList
  }

  private def transformRenameUUID(df: DataFrame): DataFrame =
    df.withColumnRenamed("student_uuid", s"${LessonAssignmentEntity}_student_id")
      .withColumnRenamed("class_uuid", s"${LessonAssignmentEntity}_class_id")
      .withColumnRenamed("lo_uuid", s"${LessonAssignmentEntity}_lo_id")
      .withColumnRenamed("teacher_uuid", s"${LessonAssignmentEntity}_teacher_id")

}

object LessonAssignmentDimension {
  val ClassTypeAssociation = 1
  val StudentTypeAssociation = 2
  def main(args: Array[String]): Unit =
    new LessonAssignmentDimension(LessonAssignmentDimensionName, SparkSessionUtils.getSession(LessonAssignmentDimensionName)).run
}
