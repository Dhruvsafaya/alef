package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.service._
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AssignmentInstanceDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import session.implicits._

  override val name: String = AssignmentInstanceDimension.name

  val deltaMatchConditions: String =
    s"""
       |${Alias.Delta}.${AssignmentInstanceStudentEntity}_instance_id = ${Alias.Events}.${AssignmentInstanceStudentEntity}_instance_id
     """.stripMargin

  override def transform(): List[Sink] = {
    //read
    val assignmentInstanceMutatedSource = readOptional(AssignmentInstanceMutatedParquetSource, session, extraProps = List(("mergeSchema", "true"))).map(_.addColIfNotExists("teachingPeriodId"))
    val assignmentInstanceDeletedSource = readOptional(AssignmentInstanceDeletedParquetSource, session, isMandatory = false)
    val assignmentInstanceStudentSource = readOptional(AssignmentInstanceStudentParquetSource, session, isMandatory = false)

    //transform for assignment instance
    val transformAssignmentInstanceMutatedDf =
      assignmentInstanceMutatedSource.map(
        _.selectLatestByRowNumber(List("id", "eventType"))
          .withColumn("startOn", when($"startOn".isNotNull, getUTCDateFrom($"startOn").cast(TimestampType)))
          .withColumn("dueOn", when($"dueOn".isNotNull, getUTCDateFrom($"dueOn").cast(TimestampType)))
          .cache()
      )
    val assignmentInstanceCreatedDf = transformAssignmentInstanceMutatedDf.map(
      _.filter($"eventType" === AssignmentInstanceCreatedEvent)
        .transformForInsertDim(AssignmentInstanceCreatedColumns, AssignmentInstanceEntity, ids = List("id"))
    )
    val assignmentInstanceUpdateSource: Option[DataFrame] = transformAssignmentInstanceMutatedDf
      .map(_.filter($"eventType" === AssignmentInstanceUpdatedEvent).cache())
      .flatMap(df => if (df.isEmpty) None else Some(df))
    val assignmentInstanceUpdatedDf: Option[DataFrame] = assignmentInstanceUpdateSource
      .map(_.transformForUpdateDim(AssignmentInstanceUpdatedColumns, AssignmentInstanceEntity, List("id")))
      .flatMap(_.checkEmptyDf)
    val assignmentInstanceDeletedDf = assignmentInstanceDeletedSource
      .map(_.transformForDelete(AssignmentInstanceDeletedColumns, AssignmentInstanceEntity))

    //transform for assignment instance student
    val studentsFromCreatedEvents = assignmentInstanceMutatedSource
      .map(_.filter($"eventType" === AssignmentInstanceCreatedEvent).select("eventType", "id", "students", "occurredOn"))
      .flatMap(_.checkEmptyDf)

    val studentsFromStudentUpdatedEvents =
      assignmentInstanceStudentSource.map(_.select("eventType", "id", "students", "occurredOn")).flatMap(_.checkEmptyDf)

    val allAssociations =
      (studentsFromCreatedEvents, studentsFromStudentUpdatedEvents) match {
        case (Some(partOne), Some(partTwo)) => Some(partOne.unionByName(partTwo))
        case (Some(partOne), None)          => Some(partOne)
        case (None, Some(partTwo))          => Some(partTwo)
        case _                              => None
      }

    val assignmentInstanceStudents = allAssociations.map(
      _.transform(commonTransformations)
        .transformForInsertDim(AssignmentInstanceStudentColumns, AssignmentInstanceStudentEntity, ids = List("id", "studentId"))
    )

    val studentsFromUpdatedEvents = assignmentInstanceMutatedSource
      .map(_.filter($"eventType" === AssignmentInstanceUpdatedEvent).select("eventType", "id", "students", "occurredOn"))
      .flatMap(_.checkEmptyDf)

    val assignmentInstanceUpdatedStudents = studentsFromUpdatedEvents.map(
      _.transform(commonTransformations)
        .transformForInsertDim(AssignmentInstanceStudentColumns, AssignmentInstanceStudentEntity, ids = List("id", "studentId"))
    )

    //sinks
    val parquetSinks = getParquetSinks(assignmentInstanceMutatedSource, assignmentInstanceDeletedSource)
    val redshiftSinks = getRedshiftSinks(assignmentInstanceCreatedDf, assignmentInstanceUpdatedDf, assignmentInstanceDeletedDf)
    val deltaSinks = getDeltaSinks(assignmentInstanceCreatedDf, assignmentInstanceUpdatedDf, assignmentInstanceDeletedDf)

    //all sinks
    val instanceSinks = parquetSinks ++ redshiftSinks ++ deltaSinks
    val instanceStudentSinks = getAssociationSinks(assignmentInstanceStudentSource, assignmentInstanceStudents, assignmentInstanceUpdatedStudents)
    instanceSinks ++ instanceStudentSinks
  }

  private def getParquetSinks(mutatedDF: Option[DataFrame], deletedDF: Option[DataFrame]): List[DataSink] = {
    val mutatedSink = mutatedDF.map(_.toParquetSink(AssignmentInstanceMutatedParquetSource))
    val deletedSink = deletedDF.map(_.toParquetSink(AssignmentInstanceDeletedParquetSource))
    (mutatedSink ++ deletedSink).toList
  }

  private def getAssociationSinks(assignmentInstanceStudentSource: Option[DataFrame],
                                  assignmentInstanceStudents: Option[DataFrame],
                                  assignmentInstanceUpdatedStudents: Option[DataFrame]): List[Sink] = {

    val AssignmentInstanceStudentEntityForCondition: String = "ais_instance"

    val parquetAssociationSinks = assignmentInstanceStudentSource
      .map(_.toParquetSink(AssignmentInstanceStudentParquetSource))

    val redshiftAssociationSinks = assignmentInstanceStudents.map(
      _.toRedshiftInsertSink(AssignmentInstanceStudentRedshiftSink, AssignmentInstanceCreatedEvent))
    val redshiftAssociationUpdateSinks = assignmentInstanceUpdatedStudents.map(
      _.toRedshiftResetSink(AssignmentInstanceStudentRedshiftSink, AssignmentInstanceUpdatedEvent,AssignmentInstanceEntity, "ais_instance"))

    val deltaAssociationSinks = assignmentInstanceStudents.flatMap(
      _.toCreate().map(_.toSink(AssignmentInstanceStudentDeltaSink)))

    val deltaAssociationUpdateSinks = assignmentInstanceUpdatedStudents.map(
      _.toResetContext(
        matchConditions = deltaMatchConditions,
        uniqueIdColumns = List(s"${AssignmentInstanceStudentEntity}_instance_id"))
    ).map(_.toSink(AssignmentInstanceStudentDeltaSink, AssignmentInstanceStudentEntityForCondition))

    (parquetAssociationSinks ++ redshiftAssociationSinks ++ deltaAssociationSinks ++ deltaAssociationUpdateSinks
      ++ redshiftAssociationUpdateSinks).toList
  }

  private def getRedshiftSinks(createdDF: Option[DataFrame], updatedDF: Option[DataFrame], deletedDF: Option[DataFrame]): List[Sink] = {
    val createSink =
      createdDF.map(
        _.drop("assignment_instance_group_id")
          .transform(renameColumnsForRS)
          .toRedshiftInsertSink(AssignmentInstanceRedshiftSink, AssignmentInstanceCreatedEvent))

    val updatedSink =
      updatedDF.map(
        _.drop("assignment_instance_group_id")
          .transform(renameColumnsForRS)
          .toRedshiftUpdateSink(AssignmentInstanceRedshiftSink, AssignmentInstanceUpdatedEvent, AssignmentInstanceEntity, isStaging = true))

    val deletedSink =
      deletedDF.map(
        _.toRedshiftUpdateSink(AssignmentInstanceRedshiftSink, AssignmentInstanceDeletedEvent, AssignmentInstanceEntity, isStaging = true))

    (createSink ++ updatedSink ++ deletedSink).toList
  }

  private def getDeltaSinks(createdDF: Option[DataFrame], updatedDF: Option[DataFrame], deletedDF: Option[DataFrame]): List[Sink] = {

    val createSink = createdDF.flatMap(_.toCreate().map(_.toSink(AssignmentInstanceDeltaSink)))
    val updatedSink = updatedDF.flatMap(
      _.toUpdate().map(_.toSink(AssignmentInstanceDeltaSink, AssignmentInstanceEntity))
    )
    val deleteMatchCondition =
      s"${Alias.Delta}.${AssignmentInstanceEntity}_id = ${Alias.Events}.${AssignmentInstanceEntity}_id and ${Alias.Delta}.${AssignmentInstanceEntity}_status = 1"
    val deletedSink = deletedDF.flatMap(
      _.toDelete(matchConditions = deleteMatchCondition).map(_.toSink(AssignmentInstanceDeltaSink, AssignmentInstanceEntity)))

    (createSink ++ updatedSink ++ deletedSink).toList
  }

  private def normalizeStudentSchema(df: DataFrame) = {
    df.schema("s").dataType match {
      case StringType => df.withColumn("s", lit(null).cast("struct<id:string,active:boolean>"))
      case _          => df
    }
  }

  def commonTransformations(df: DataFrame): DataFrame = {
    val instanceWithStudents = df.filter(size($"students")>0)
    val exploded = instanceWithStudents
      .select($"*", explode($"students").as("s"))
    normalizeStudentSchema(exploded)
      .withColumn("studentId", $"s.id")
      .withColumn("active", $"s.active")
  }

  private def renameColumnsForRS(df: DataFrame): DataFrame = {
    val newColsExprQry = df.columns.map(c => col(c).alias(AssignmentInstanceDimension.renameColumnForRS(c)))
    df.select(newColsExprQry: _*)
  }
}

object AssignmentInstanceDimension {

  private val name: String = "assignment-instance-dimension"

  def renameColumnForRS(c: String): String = {
    if (c != "assignment_instance_id" && c != "assignment_instance_instructional_plan_id" && c != "assignment_instance_trimester_id"
      && c != "assignment_instance_teaching_period_id" && c.contains("_id"))
      c.replace("assignment_instance_", "").replace("_id", "_uuid")
    else c
  }

  def apply(implicit session: SparkSession): AssignmentInstanceDimension = new AssignmentInstanceDimension

  def main(args: Array[String]): Unit = {
    AssignmentInstanceDimension(SparkSessionUtils.getSession(name)).run
  }

}
