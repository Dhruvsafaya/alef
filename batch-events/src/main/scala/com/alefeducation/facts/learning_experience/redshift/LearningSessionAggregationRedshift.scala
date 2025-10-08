package com.alefeducation.facts.learning_experience.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.service.DataSink
import com.alefeducation.facts.learning_experience.redshift.LearningSessionAggregationRedshift.{LearningSessionRedshiftCols, LearningSessionRedshiftSink}
import com.alefeducation.facts.learning_experience.transform.LearningSessionAggregation.TransformedLearningSessionSink
import com.alefeducation.util.DataFrameUtility.selectAs
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class LearningSessionAggregationRedshift(val session: SparkSession,
                                         val service: SparkBatchService) {
  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(TransformedLearningSessionSink, session)

    val learningSessionRedshiftDF = dataFrame.map(selectAs(_, LearningSessionRedshiftCols))

    learningSessionRedshiftDF.map(DataSink(LearningSessionRedshiftSink, _))
  }
}

object LearningSessionAggregationRedshift {

  val LearningSessionRedshiftService = "redshift-learning-session"

  val LearningSessionRedshiftSink = "redshift-learning-experience-sink"

  val session = SparkSessionUtils.getSession(LearningSessionRedshiftService)
  val service = new SparkBatchService(LearningSessionRedshiftService, session)

  val LearningSessionRedshiftCols = Map(
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
    "fle_source" -> "fle_source",
    "fle_bonus_stars" -> "fle_bonus_stars",
    "fle_bonus_stars_scheme" -> "fle_bonus_stars_scheme"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new LearningSessionAggregationRedshift(session, service)
    service.run(transformer.transform())
  }
}

