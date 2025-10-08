package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.DataFrameUtility.{getUTCDateFrom, _}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AssignmentSubmissionMutatedEvent
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AssignmentSubmission(override implicit val session: SparkSession) extends SparkBatchService {

  import session.implicits._

  override val name: String = AssignmentSubmission.name

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._

    val assignmentSubmissionMutatedSource = readOptional(AssignmentSubmissionSource, session, isMandatory = false)

    val assignmentSubmissionTransformed = assignmentSubmissionMutatedSource.map(
      _.transformIfNotEmpty(transformSubmission)
        .transformForInsertFact(AssignmentSubmissionColumns, AssignmentSubmissionEntity))

    val parquetSinks = assignmentSubmissionMutatedSource.map(_.toParquetSink(AssignmentSubmissionSource)).toList
    val deltaSinks = getDeltaSinks(assignmentSubmissionTransformed)
    val redshiftSinks = getRedshiftSinks(assignmentSubmissionTransformed)
    parquetSinks ++ redshiftSinks  ++ deltaSinks
  }

  private def getDeltaSinks(assignmentSubmissionDf: Option[DataFrame]): List[Sink] = {
    val assignmentSubmissionSink =
      assignmentSubmissionDf.flatMap(_.toCreate(isFact = true).map(_.toSink(AssignmentSubmissionDeltaSink)))
    assignmentSubmissionSink.toList
  }

  private def getRedshiftSinks(assignmentSubmissionDF: Option[DataFrame]): List[Sink] = {

    assignmentSubmissionDF
      .map(_.transform(transformCommentColumns))
      .map(_.toRedshiftInsertSink(AssignmentSubmissionRedshiftSink, AssignmentSubmissionMutatedEvent))
      .toList

  }

  private def transformSubmission(df: DataFrame): DataFrame = {
    df.withColumn("updatedOn", when($"updatedOn".isNotNull, getUTCDateFrom($"updatedOn").cast(TimestampType)))
      .withColumn("submittedOn", when($"submittedOn".isNotNull, getUTCDateFrom($"submittedOn").cast(TimestampType)))
      .withColumn("gradedOn", when($"gradedOn".isNotNull, getUTCDateFrom($"gradedOn").cast(TimestampType)))
      .withColumn("evaluatedOn", when($"evaluatedOn".isNotNull, getUTCDateFrom($"evaluatedOn").cast(TimestampType)))
      .withColumn("returnedOn", when($"returnedOn".isNotNull, getUTCDateFrom($"returnedOn").cast(TimestampType)))
      .addColIfNotExists("resubmissionCount", IntegerType)
  }

  private def transformCommentColumns(dataFrame: DataFrame): DataFrame = {

    dataFrame.withColumn("assignment_submission_has_teacher_comment", $"assignment_submission_teacher_comment".isNotNull)
      .withColumn("assignment_submission_has_student_comment", $"assignment_submission_student_comment".isNotNull)
      .drop("assignment_submission_student_comment", "assignment_submission_teacher_comment")

  }

}

object AssignmentSubmission {

  private val name = AssignmentSubmissionName

  def apply(implicit session: SparkSession): AssignmentSubmission = new AssignmentSubmission

  def main(args: Array[String]): Unit = {
    AssignmentSubmission(SparkSessionUtils.getSession(name)).run
  }

}
