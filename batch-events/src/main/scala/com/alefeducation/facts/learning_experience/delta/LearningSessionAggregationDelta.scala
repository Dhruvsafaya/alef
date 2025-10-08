package com.alefeducation.facts.learning_experience.transform.delta

import com.alefeducation.base.SparkBatchService
import org.apache.spark.sql.SparkSession
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.learning_experience.transform.LearningSessionAggregation.TransformedLearningSessionSink
import com.alefeducation.schema.lps.ScoreBreakdown
import com.alefeducation.util.DataFrameUtility.selectAs
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{date_format, typedLit}

class LearningSessionAggregationDelta(val session: SparkSession,
                                      val service: SparkBatchService) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  import session.implicits._
  import LearningSessionAggregationDelta.{LearningSessionDeltaCols, LearningSessionDeltaSink}

  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(TransformedLearningSessionSink, session)

    val selectFieldsForDelta = LearningSessionDeltaCols.map { case (k, v) => (k, v.replace("_uuid", "_id")) }

    val learningSessionDeltaDF = dataFrame.map(selectAs(_, selectFieldsForDelta)
      .withColumn(s"${FactLearningExperienceEntity}_score_breakdown", typedLit(Seq.empty[ScoreBreakdown]))
      .withColumn("eventdate", date_format($"fle_created_time", "yyyy-MM-dd"))
    )

    learningSessionDeltaDF.flatMap(_.toCreate(isFact = true).map(_.toSink(LearningSessionDeltaSink)))
  }
}

object LearningSessionAggregationDelta {

  val LearningSessionDeltaService = "delta-learning-session"

  val LearningSessionDeltaSink = "delta-learning-experience-sink"

  val session = SparkSessionUtils.getSession(LearningSessionDeltaService)
  val service = new SparkBatchService(LearningSessionDeltaService, session)

  val LearningSessionDeltaCols = Map(
    "fle_material_id" -> "fle_material_id",
    "curr_grade_uuid" -> "curr_grade_uuid",
    "eventType" -> "eventType",
    "fle_date_dw_id" -> "fle_date_dw_id",
    "fle_activity_component_type" -> "fle_activity_component_type",
    "fle_start_time" -> "fle_start_time",
    "fle_exit_ticket" -> "fle_exit_ticket",
    "fle_star_earned" -> "fle_star_earned",
    "grade_uuid" -> "grade_uuid",
    "fle_state" -> "fle_state",
    "fle_exp_id" -> "fle_exp_id",
    "lp_uuid" -> "lp_uuid",
    "fle_main_component" -> "fle_main_component",
    "school_uuid" -> "school_uuid",
    "fle_activity_template_id" -> "fle_activity_template_id",
    "fle_lesson_category" -> "fle_lesson_category",
    "fle_score" -> "fle_score",
    "academic_year_uuid" -> "academic_year_uuid",
    "fle_completion_node" -> "fle_completion_node",
    "fle_material_type" -> "fle_material_type",
    "fle_time_spent_app" -> "fle_time_spent_app",
    "section_uuid" -> "section_uuid",
    "fle_instructional_plan_id" -> "fle_instructional_plan_id",
    "subject_uuid" -> "subject_uuid",
    "fle_lesson_type" -> "fle_lesson_type",
    "fle_end_time" -> "fle_end_time",
    "fle_activity_type" -> "fle_activity_type",
    "fle_content_academic_year" -> "fle_content_academic_year",
    "fle_outside_of_school" -> "fle_outside_of_school",
    "fle_total_score" -> "fle_total_score",
    "fle_total_stars" -> "fle_total_stars",
    "fle_ls_id" -> "fle_ls_id",
    "fle_academic_period_order" -> "fle_academic_period_order",
    "curr_subject_uuid" -> "curr_subject_uuid",
    "fle_exp_ls_flag" -> "fle_exp_ls_flag",
    "fle_step_id" -> "fle_step_id",
    "fle_is_activity_completed" -> "fle_is_activity_completed",
    "fle_attempt" -> "fle_attempt",
    "tenant_uuid" -> "tenant_uuid",
    "fle_total_time" -> "fle_total_time",
    "fle_abbreviation" -> "fle_abbreviation",
    "fle_is_retry" -> "fle_is_retry",
    "class_uuid" -> "class_uuid",
    "lo_uuid" -> "lo_uuid",
    "student_uuid" -> "student_uuid",
    "curr_uuid" -> "curr_uuid",
    "eventdate" -> "eventdate",
    "fle_created_time" -> "fle_created_time",
    "fle_dw_created_time" -> "fle_dw_created_time",
    "fle_activity_component_resource" -> "fle_activity_component_resource",
    "fle_bonus_stars" -> "fle_bonus_stars",
    "fle_bonus_stars_scheme" -> "fle_bonus_stars_scheme"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new LearningSessionAggregationDelta(session, service)
    service.run(transformer.transform())
  }
}

