package com.alefeducation.dimensions.question_pool

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.service.DataSink
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap

class QuestionPoolDimensionTransform(val session: SparkSession, val service: SparkBatchService) {
  import QuestionPoolDimensionTransform._

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._
    import org.apache.spark.sql.functions.{col, coalesce}

    val questionPoolDF: Option[DataFrame] = service.readOptional(QuestionPoolMutatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val transformedDF = questionPoolDF.map(
      _.transformForInsertOrUpdate(
        QuestionPoolMutatedColumnMapping,
        QuestionPoolEntityPrefix,
        List("poolId")
      ).withColumn("question_pool_updated_time", col("question_pool_created_time"))
      .withColumn("question_pool_dw_updated_time", col("question_pool_dw_created_time"))
    )

    List(
      transformedDF.map(DataSink(QuestionPoolMutatedSinkName, _))
    )
  }
}

object QuestionPoolDimensionTransform {

  private val QuestionPoolMutatedTransformService: String = "transform-ccl-question-pool-mutated-service"
  val QuestionPoolEntityPrefix: String = "question_pool"

  val QuestionPoolMutatedSourceName: String = getSource(QuestionPoolMutatedTransformService).head
  val QuestionPoolMutatedSinkName: String = getSink(QuestionPoolMutatedTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(QuestionPoolMutatedTransformService)
  val service = new SparkBatchService(QuestionPoolMutatedTransformService, session)


  private val QuestionPoolMutatedColumnMapping: Map[String, String] = ListMap(
    "poolId" -> s"${QuestionPoolEntityPrefix}_id",
    "name" -> s"${QuestionPoolEntityPrefix}_name",
    "triggeredBy" -> s"${QuestionPoolEntityPrefix}_triggered_by",
    "questionCodePrefix" -> s"${QuestionPoolEntityPrefix}_question_code_prefix",
    "status" -> s"${QuestionPoolEntityPrefix}_app_status",
    s"${QuestionPoolEntityPrefix}_status" -> s"${QuestionPoolEntityPrefix}_status",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new QuestionPoolDimensionTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
