package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{ getList, getNestedConfig, getNestedMap, getNestedString, getString}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.TimestampType

class TutorChallengeQuestionAnswer(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[Sink] = {
    val key = getNestedString(serviceName, "key")
    val startId = service.getStartIdUpdateStatus(key)
    val questionSource = service.readOptional(getString(serviceName, "source"), session, extraProps = List(("mergeSchema", "true")))

    val columnMapping = getNestedMap(serviceName, "column-mapping")
    val additional = getNestedConfig(serviceName, "additional")
    val isAnswerEvaluated = additional.getOrElse("is_answer_evaluated", false).asInstanceOf[Boolean]
    val timestampColumn = additional.getOrElse("timestamp_column", "ftcqa_bot_question_timestamp").asInstanceOf[String]
    val entityName = getNestedString(serviceName, "entity")
    val uniqueId = getList(serviceName, "unique-ids")
    val sinkName = getString(serviceName, "sink")

    val questionDf = questionSource.map(_
      .transformForInsertFact(columnMapping, entityName, uniqueId)
      .withColumn("ftcqa_is_answer_evaluated", lit(isAnswerEvaluated))
      .withColumn(timestampColumn, col(timestampColumn).cast(TimestampType))
    )

    val df = questionDf.map(
      _.genDwId(s"${entityName}_dw_id", startId, orderByField = s"${entityName}_created_time")
    )

    df.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key)))
  }
}

object TutorChallengeQuestionAnswer {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new TutorChallengeQuestionAnswer(session, service, args(0))
    service.run(transformer.transform())
  }

}
