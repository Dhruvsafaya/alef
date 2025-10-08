package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MetadataBuilder, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AssignmentDimension(override implicit val session: SparkSession) extends SparkBatchService {
  private val colLength = 500
  private val metadata = new MetadataBuilder().putLong("maxlength", colLength).build()

  import session.implicits._
  override val name: String = AssignmentDimension.name

  override def transform(): List[Sink] = {
    val mutatedSource = readOptional(AssignmentMutatedParquetSource, session, isMandatory = false)
    val deletedSource = readOptional(AssignmentDeletedParquetSource, session, isMandatory = false)
    val mutatedAlefAssignmentSource = readOptional(AlefAssignmentMutatedParquetSource, session, isMandatory = false)
    val deletedAlefAssignmentSource = readOptional(AlefAssignmentDeletedParquetSource, session, isMandatory = false)

    val assignmentCreated =
      mutatedSource.flatMap(
        _.filter($"eventType" === AssignmentCreatedEvent)
          .transformIfNotEmpty(transformMutatedDf)
          .transformForInsertDim(AssignmentMutatedColumns, AssignmentEntity, ids = List("id"))
          .checkEmptyDf
      )

    val assignmentUpdated =
      mutatedSource.flatMap(
        _.filter($"eventType" === AssignmentUpdatedEvent)
          .transformIfNotEmpty(transformMutatedDf)
          .transformForUpdateDim(AssignmentUpdatedColumns, AssignmentEntity, ids = List("id"))
          .checkEmptyDf
      )

    val alefAssignmentCreated =
      mutatedAlefAssignmentSource.flatMap(
        _.filter($"eventType" === AssignmentCreatedEvent)
          .transformIfNotEmpty(transformMutatedDf)
          .transformForInsertDim(AlefAssignmentMutatedColumns, AssignmentEntity, ids = List("id"))
          .checkEmptyDf
      )

    val alefAssignmentUpdated =
      mutatedAlefAssignmentSource.flatMap(
        _.filter($"eventType" === AssignmentUpdatedEvent)
          .transformIfNotEmpty(transformMutatedDf)
          .transformForUpdateDim(AlefAssignmentUpdatedColumns, AssignmentEntity, ids = List("id"))
          .checkEmptyDf
      )

    val teacherAssignmentDeleted =
      deletedSource.map(_.transformForDelete(AssignmentDeletedColumns, AssignmentEntity))

    val alefAssignmentDeleted =
      deletedAlefAssignmentSource.map(_.transformForDelete(AssignmentDeletedColumns, AssignmentEntity))

    val assignmentDeleted = combineOptionalDfs(teacherAssignmentDeleted, alefAssignmentDeleted)

    getParquetSinks(mutatedSource, deletedSource, mutatedAlefAssignmentSource, deletedAlefAssignmentSource) ++
      getDeltaSinks(assignmentCreated, assignmentUpdated, alefAssignmentCreated, alefAssignmentUpdated, assignmentDeleted) ++
      getRedshiftSinks(assignmentCreated, assignmentUpdated, alefAssignmentCreated, alefAssignmentUpdated, assignmentDeleted)
  }

  private def getParquetSinks(mutatedSource: Option[DataFrame],
                              deletedSource: Option[DataFrame],
                              mutatedAlefAssignmentSource: Option[DataFrame],
                              deletedAlefAssignmentSource: Option[DataFrame]): List[Sink] = {
    val mutatedSink = mutatedSource.map(_.toParquetSink(AssignmentMutatedParquetSource))
    val deletedSink = deletedSource.map(_.toParquetSink(AssignmentDeletedParquetSource))
    val mutatedAlefAssignmentSink = mutatedAlefAssignmentSource.map(_.toParquetSink(AlefAssignmentMutatedParquetSource))
    val deletedAlefAssignmentSink = deletedAlefAssignmentSource.map(_.toParquetSink(AlefAssignmentDeletedParquetSource))
    (mutatedSink ++ deletedSink ++ mutatedAlefAssignmentSink ++ deletedAlefAssignmentSink).toList
  }

  private def getDeltaSinks(created: Option[DataFrame],
                            updated: Option[DataFrame],
                            alefAssignmentCreated: Option[DataFrame],
                            alefAssignmentUpdated: Option[DataFrame],
                            deleted: Option[DataFrame]): List[Sink] = {
    val createdSink = created.flatMap(_.toCreate().map(_.toSink(AssignmentDeltaSink)))
    val updateSink = updated.flatMap(_.toUpdate().map(_.toSink(AssignmentDeltaSink, AssignmentEntity)))
    val createdAlefAssignmentSink = alefAssignmentCreated.flatMap(_.toCreate().map(_.toSink(AssignmentDeltaSink)))
    val updateAlefAssignmentSink = alefAssignmentUpdated.flatMap(_.toUpdate().map(_.toSink(AssignmentDeltaSink, AssignmentEntity)))
    val deletedSink = deleted.flatMap(_.toDelete().map(_.toSink(AssignmentDeltaSink, AssignmentEntity)))
    (createdSink ++ updateSink ++ createdAlefAssignmentSink ++ updateAlefAssignmentSink ++ deletedSink).toList
  }

  private def getRedshiftSinks(created: Option[DataFrame],
                               updated: Option[DataFrame],
                               alefAssignmentCreated: Option[DataFrame],
                               alefAssignmentUpdated: Option[DataFrame],
                               deleted: Option[DataFrame]): List[Sink] = {
    val columnsToBeDropped = List(
      "assignment_description",
      "assignment_metadata_keywords",
      "assignment_metadata_cognitive_dimensions",
      "assignment_metadata_copyrights",
      "assignment_metadata_conditions_of_use",
      "assignment_metadata_curriculum_outcomes",
      "assignment_metadata_skills"
    )
    val createdSink = created.map(_.drop("assignment_description")
      .toRedshiftInsertSink(AssignmentRedshiftSink, AssignmentCreatedEvent))
    val updateSink = updated.map(
      _.drop("assignment_description")
        .toRedshiftUpdateSink(AssignmentRedshiftSink, AssignmentUpdatedEvent, AssignmentEntity, List(s"${AssignmentEntity}_id"), isStaging = true))
    val createdAlefAssignmentSink =
      alefAssignmentCreated.map(
        _.drop(columnsToBeDropped: _*).toRedshiftInsertSink(AssignmentRedshiftSink, AssignmentCreatedEvent))
    val updateAlefAssignmentSink =
      alefAssignmentUpdated.map(
        _.drop(columnsToBeDropped: _*).toRedshiftUpdateSink(AssignmentRedshiftSink,
                                                            AssignmentUpdatedEvent,
                                                            AssignmentEntity,
                                                            List(s"${AssignmentEntity}_id"),
                                                            isStaging = true))
    val deletedSink = deleted.map(
      _.toRedshiftUpdateSink(AssignmentRedshiftSink, AssignmentDeletedEvent, AssignmentEntity, List(s"${AssignmentEntity}_id"), isStaging = true))
    (createdSink ++ updateSink ++ createdAlefAssignmentSink ++ updateAlefAssignmentSink ++ deletedSink).toList
  }

  private def transformMutatedDf(df: DataFrame): DataFrame = {
    df.withColumn("publishedOn", when($"publishedOn".isNotNull, getUTCDateFrom($"publishedOn").cast(TimestampType)))
      .withColumn("attachment.fileId", when($"attachment".isNull, lit(null).cast(StringType)).otherwise($"attachment.fileId"))
      .withColumn("attachment.fileName",
        when($"attachment".isNull, lit(null).cast(StringType))
          .otherwise($"attachment.fileName")
          .as("attachment.fileName", metadata))
      .withColumn("attachment.path",
        when($"attachment".isNull, lit(null).cast(StringType))
          .otherwise($"attachment.path")
          .as("attachment.path", metadata))
  }

}

object AssignmentDimension {

  val name: String = AssignmentDimensionName

  def apply(implicit session: SparkSession): AssignmentDimension = new AssignmentDimension

  def main(args: Array[String]): Unit = {
    AssignmentDimension(SparkSessionUtils.getSession(name)).run
  }

}
