package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Builder, Delta, DeltaSink}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import com.alefeducation.util.DataFrameUtility._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap

class QuestionPoolAssociationDimension(override implicit val session: SparkSession) extends SparkBatchService {
  import QuestionPoolAssociationDimension._
  import session.implicits._
  override val name: String = JobName

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._
    import Builder.Transformations
    import Delta.Transformations._

    val parquetAssociationDF = readOptional(Parquet.AssociationSourceConfig, session).map(_.cache())

    val outputIWHDF = parquetAssociationDF.map(
      _.dropDuplicates("poolId","questions", "occurredOn")
        .transform(transformStatusesAndAssociationType)
        .transformForInsertWithHistory(QuestionPoolAssociationMutatedColumnMapping, EntityName)
        .cache())
    
    val deltaMatchConditions: String =
      s"""
         |${Alias.Delta}.${EntityName}_pool_id = ${Alias.Events}.${EntityName}_pool_id
         | and
         |${Alias.Delta}.${EntityName}_question_code = ${Alias.Events}.${EntityName}_question_code
     """.stripMargin

    val deltaIWHSink: Option[DeltaSink] = outputIWHDF
      .flatMap(df =>
        df.sort(orderByField)
          .withEventDateColumn(false)
          .toIWH(
            matchConditions = deltaMatchConditions,
            uniqueIdColumns = uniqueIdColumns)
          .map(_.toSink(DeltaSink, EntityName)))
    val transformedDF = outputIWHDF.map(_.withColumnRenamed("question_pool_association_pool_id", "question_pool_uuid"))

    val redshiftIWHSink = transformedDF.map(df => df.withEventDateColumn(false)
      .toRedshiftIWHSink(RedshiftSink, EntityName, ids = List("question_pool_uuid", "question_pool_association_question_code"),  isStagingSink = true))
    
    val parquetSink = parquetAssociationDF.map(_.toParquetSink(Parquet.AssociationSourceConfig))

    (deltaIWHSink ++ parquetSink ++ redshiftIWHSink).toList
  }

  def transformStatusesAndAssociationType(df: DataFrame): DataFrame = {
    val explodedDf = df.select($"*", explode($"questions").as("question"))
      .select($"*", $"question.code".as("questionCode")).drop(s"question")
    val latestUpdatedDf = selectLatestRecords(explodedDf,  List("poolId", "questionCode", "eventType"))
    (addStatusCol(EntityName, orderByField, uniqueKey) _ andThen
      addUpdatedTimeColsForAssociation(EntityName, orderByField, uniqueKey) andThen
      transformAssociationAttachStatus)(latestUpdatedDf)
  }

  def transformAssociationAttachStatus(df: DataFrame): DataFrame =
    df.withColumn("question_pool_association_assign_status",
      when(col("eventType") === "QuestionsAddedToPoolEvent", ASSIGNED).otherwise(UNASSIGNED))

}

object QuestionPoolAssociationDimension {

  val JobName: String = "question-pool-association-dimension"
  val EntityName: String = "question_pool_association"

  val QuestionsAddedToPoolEvent: String = "QuestionsAddedToPoolEvent"
  val QuestionsRemovedFromPoolEvent: String = "QuestionsRemovedFromPoolEvent"

  val QuestionPoolAssociationMutatedColumnMapping: Map[String, String] = ListMap(
    "triggeredBy" -> s"${EntityName}_triggered_by",
    "questionCode" -> s"${EntityName}_question_code",
    "poolId" -> s"${EntityName}_pool_id",
    s"${EntityName}_status" -> s"${EntityName}_status",
    s"${EntityName}_assign_status" -> s"${EntityName}_assign_status",
    "occurredOn" -> "occurredOn"
  )

  val DeltaSink: String = "delta-question-pool-association-sink"
  val RedshiftSink: String = "redshift-question-pool-association-sink"
  val ASSIGNED = 1
  val UNASSIGNED = 0
  val orderByField = "occurredOn"
  val uniqueKey = List("poolId", "questionCode")
  val uniqueIdColumns = List(s"${EntityName}_pool_id", s"${EntityName}_question_code")

  object Parquet {
    val AssociationSourceConfig: String = "parquet-question-pool-association"
  }

  def apply(implicit session: SparkSession): QuestionPoolAssociationDimension = new QuestionPoolAssociationDimension

  def main(args: Array[String]): Unit = QuestionPoolAssociationDimension(SparkSessionUtils.getSession(JobName)).run

}
