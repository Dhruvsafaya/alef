package com.alefeducation.facts.adt_next_question.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.adt_next_question.redshift.ADTNextQuestionRedshift.ADTNextQuestionRedshiftSink
import com.alefeducation.facts.adt_next_question.transform.ADTNextQuestionTransform.ADTNextQuestionTransformedSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class ADTNextQuestionRedshift(val session: SparkSession,
                              val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(ADTNextQuestionTransformedSink, session).map(_.drop("fanq_breakdown"))

    dataFrame.map(DataSink(ADTNextQuestionRedshiftSink, _))

  }
}

object ADTNextQuestionRedshift {

  val ADTNextQuestionRedshiftService = "redshift-adt-next-question"
  val ADTNextQuestionRedshiftSink = "redshift-next-question-sink"


  val session = SparkSessionUtils.getSession(ADTNextQuestionRedshiftService)
  val service = new SparkBatchService(ADTNextQuestionRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ADTNextQuestionRedshift(session, service)
    service.run(transformer.transform())
  }
}
