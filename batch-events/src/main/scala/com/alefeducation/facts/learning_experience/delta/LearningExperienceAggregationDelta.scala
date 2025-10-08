package com.alefeducation.facts.learning_experience.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.learning_experience.delta.LearningExperienceAggregationDelta.{LearningExperienceDeltaSink, LearningExperienceDeltaCols}
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.TransformedLearningExperienceSink
import com.alefeducation.util.DataFrameUtility.selectAs
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class LearningExperienceAggregationDelta(val session: SparkSession,
                                         val service: SparkBatchService) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def transform(): Option[Sink] = {
    val dataFrame = service.readOptional(TransformedLearningExperienceSink, session)

    val learningExperienceDeltaDF = dataFrame.map(selectAs(_, LearningExperienceDeltaCols))

    learningExperienceDeltaDF.flatMap(_.toCreate(isFact = true).map(_.toSink(LearningExperienceDeltaSink)))
  }
}

object LearningExperienceAggregationDelta {

  val LearningExperienceDeltaService = "delta-learning-experience"

  val LearningExperienceDeltaSink = "delta-learning-experience-sink"

  val session = SparkSessionUtils.getSession(LearningExperienceDeltaService)
  val service = new SparkBatchService(LearningExperienceDeltaService, session)

  val LearningExperienceDeltaCols = Map(
    "fle_material_id"->"fle_material_id",
    "curr_grade_uuid"->"curr_grade_id",
    "eventType"->"eventType",
    "fle_date_dw_id"->"fle_date_dw_id",
    "fle_activity_component_type"->"fle_activity_component_type",
    "fle_start_time"->"fle_start_time",
    "fle_exit_ticket"->"fle_exit_ticket",
    "fle_star_earned"->"fle_star_earned",
    "grade_uuid"->"grade_id",
    "fle_state"->"fle_state",
    "fle_exp_id"->"fle_exp_id",
    "lp_uuid"->"lp_id",
    "fle_main_component"->"fle_main_component",
    "school_uuid"->"school_id",
    "fle_activity_component_resource"->"fle_activity_component_resource",
    "fle_activity_template_id"->"fle_activity_template_id",
    "fle_lesson_category"->"fle_lesson_category",
    "fle_score"->"fle_score",
    "academic_year_uuid"->"academic_year_id",
    "fle_completion_node"->"fle_completion_node",
    "fle_material_type"->"fle_material_type",
    "fle_time_spent_app"->"fle_time_spent_app",
    "section_uuid"->"section_id",
    "fle_instructional_plan_id"->"fle_instructional_plan_id",
    "subject_uuid"->"subject_id",
    "fle_lesson_type"->"fle_lesson_type",
    "fle_end_time"->"fle_end_time",
    "fle_activity_type"->"fle_activity_type",
    "fle_content_academic_year"->"fle_content_academic_year",
    "fle_outside_of_school"->"fle_outside_of_school",
    "fle_total_score"->"fle_total_score",
    "fle_total_stars" -> "fle_total_stars",
    "fle_ls_id"->"fle_ls_id",
    "fle_academic_period_order"->"fle_academic_period_order",
    "curr_subject_uuid"->"curr_subject_id",
    "fle_exp_ls_flag"->"fle_exp_ls_flag",
    "fle_step_id"->"fle_step_id",
    "fle_is_activity_completed"->"fle_is_activity_completed",
    "fle_score_breakdown"->"fle_score_breakdown",
    "fle_attempt"->"fle_attempt",
    "tenant_uuid"->"tenant_id",
    "fle_total_time"->"fle_total_time",
    "fle_abbreviation"->"fle_abbreviation",
    "fle_is_retry"->"fle_is_retry",
    "class_uuid"->"class_id",
    "lo_uuid"->"lo_id",
    "student_uuid"->"student_id",
    "curr_uuid"->"curr_id",
    "fle_adt_level"->"fle_adt_level",
    "eventdate"->"eventdate",
    "fle_created_time"->"fle_created_time",
    "fle_dw_created_time"->"fle_dw_created_time",
    "fle_assessment_id"->"fle_assessment_id",
    "fle_bonus_stars" -> "fle_bonus_stars",
    "fle_bonus_stars_scheme" -> "fle_bonus_stars_scheme"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new LearningExperienceAggregationDelta(session, service)
    service.run(transformer.transform())
  }
}

