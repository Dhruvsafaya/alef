package com.alefeducation.facts.learning_experience.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.learning_experience.redshift.LearningExperienceAggregationRedshift.LearningExperienceRedshiftSink
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.TransformedLearningExperienceSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class LearningExperienceAggregationRedshift(val session: SparkSession,
                                            val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(TransformedLearningExperienceSink, session).map(_.drop("eventdate", "fle_score_breakdown", "fle_activity_component_resource"))

    dataFrame.map(DataSink(LearningExperienceRedshiftSink, _))
  }
}

object LearningExperienceAggregationRedshift {

  val LearningExperienceRedshiftService = "redshift-learning-experience"

  val LearningExperienceRedshiftSink = "redshift-learning-experience-sink"

  val session = SparkSessionUtils.getSession(LearningExperienceRedshiftService)
  val service = new SparkBatchService(LearningExperienceRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new LearningExperienceAggregationRedshift(session, service)
    service.run(transformer.transform())
  }
}

