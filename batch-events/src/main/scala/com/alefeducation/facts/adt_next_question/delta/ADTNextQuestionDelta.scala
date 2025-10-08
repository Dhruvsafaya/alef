package com.alefeducation.facts.adt_next_question.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.adt_next_question.delta.ADTNextQuestionDelta.ADTNextQuestionDeltaSink
import com.alefeducation.facts.adt_next_question.transform.ADTNextQuestionTransform.ADTNextQuestionTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class ADTNextQuestionDelta(val session: SparkSession,
                           val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(ADTNextQuestionTransformedSink, session).map(
      _.withColumnRenamed("fle_ls_uuid", s"fanq_fle_ls_id")
        .withColumnRenamed("student_uuid", s"fanq_student_id")
        .withColumnRenamed("tenant_uuid", s"fanq_tenant_id")
    )

    dataFrame.map(DataSink(ADTNextQuestionDeltaSink, _))

  }
}

object ADTNextQuestionDelta {

  val ADTNextQuestionDeltaService = "delta-adt-next-question"
  val ADTNextQuestionDeltaSink = "delta-next-question-sink"


  val session: SparkSession = SparkSessionUtils.getSession(ADTNextQuestionDeltaService)
  val service = new SparkBatchService(ADTNextQuestionDeltaService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ADTNextQuestionDelta(session, service)
    service.run(transformer.transform())
  }
}
