package com.alefeducation.warehouse.activity_settings

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.{AnyFunSuite, AnyFunSuiteLike}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ActivitySettingsTransformerTest extends AnyFunSuite with Matchers {
  val transformer: ActivitySettingsTransformer.type = ActivitySettingsTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("test_schema")

    val expectedQuery =
      s"""
         |SELECT fas_dw_id
         |FROM test_schema_stage.staging_activity_setting staging
         |         JOIN test_schema.dim_tenant t ON t.tenant_id = staging.fas_tenant_id
         |         JOIN test_schema.dim_school s ON s.school_id = staging.fas_school_id
         |         JOIN test_schema_stage.rel_user te ON te.user_id = staging.fas_teacher_id
         |         JOIN test_schema_stage.rel_dw_id_mappings dwidm ON dwidm.id = staging.fas_class_id and dwidm.entity_type = 'class'
         |         LEFT JOIN (SELECT lo_id, lo_dw_id
         |                    FROM (SELECT lo_id,
         |                                 lo_dw_id,
         |                                 row_number() OVER (PARTITION BY lo_id ORDER BY lo_created_time DESC) AS rank
         |                          FROM test_schema.dim_learning_objective) r
         |                    WHERE r.rank = 1) AS lesson ON lesson.lo_id = staging.fas_activity_id
         |         LEFT JOIN test_schema.dim_interim_checkpoint ic on ic.ic_id = staging.fas_activity_id
         |         JOIN test_schema.dim_grade g ON g.grade_id = staging.fas_grade_id
         |WHERE (lesson.lo_id IS NOT NULL OR ic.ic_id IS NOT NULL)
         |ORDER BY fas_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("test_schema", List(301, 302, 303))
    val expectedQuery =
      """
        |INSERT INTO test_schema.fact_activity_setting ( fas_activity_dw_id, fas_activity_id, fas_class_dw_id,
        |                                               fas_class_gen_subject_name, fas_class_id, fas_created_time,
        |                                               fas_dw_created_time, fas_dw_id, fas_grade_dw_id, fas_grade_id,
        |                                               fas_k12_grade, fas_school_dw_id, fas_school_id, fas_teacher_dw_id,
        |                                               fas_teacher_id, fas_tenant_dw_id, fas_tenant_id )
        |SELECT NVL(lesson.lo_dw_id, ic.ic_dw_id) AS fas_activity_dw_id,
        |       fas_activity_id,
        |       dwidm.dw_id                       AS fas_class_dw_id,
        |       fas_class_gen_subject_name,
        |       fas_class_id,
        |       fas_created_time,
        |       fas_dw_created_time,
        |       fas_dw_id,
        |       g.grade_dw_id                     AS fas_grade_dw_id,
        |       fas_grade_id,
        |       fas_k12_grade,
        |       s.school_dw_id                    AS fas_school_dw_id,
        |       fas_school_id,
        |       te.user_dw_id                     AS fas_teacher_dw_id,
        |       fas_teacher_id,
        |       t.tenant_dw_id                    AS fas_tenant_dw_id,
        |       fas_tenant_id
        |FROM test_schema_stage.staging_activity_setting staging
        |         JOIN test_schema.dim_tenant t ON t.tenant_id = staging.fas_tenant_id
        |         JOIN test_schema.dim_school s ON s.school_id = staging.fas_school_id
        |         JOIN test_schema_stage.rel_user te ON te.user_id = staging.fas_teacher_id
        |         JOIN test_schema_stage.rel_dw_id_mappings dwidm
        |              ON dwidm.id = staging.fas_class_id and dwidm.entity_type = 'class'
        |         LEFT JOIN (SELECT lo_id, lo_dw_id
        |                    FROM (SELECT lo_id,
        |                                 lo_dw_id,
        |                                 row_number() OVER (PARTITION BY lo_id ORDER BY lo_created_time DESC) AS rank
        |                          FROM test_schema.dim_learning_objective) r
        |                    WHERE r.rank = 1) AS lesson ON lesson.lo_id = staging.fas_activity_id
        |         LEFT JOIN test_schema.dim_interim_checkpoint ic on ic.ic_id = staging.fas_activity_id
        |         JOIN test_schema.dim_grade g ON g.grade_id = staging.fas_grade_id
        |WHERE (lesson.lo_id IS NOT NULL OR ic.ic_id IS NOT NULL)
        |  AND staging.fas_dw_id IN (301,302,303)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
