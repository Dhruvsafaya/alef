package com.alefeducation.warehouse.pathway

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PathwayTeacherActivityTransformerTest  extends AnyFunSuite with Matchers {

  val transformer = PathwayTeacherActivityTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""
         |SELECT fpta_dw_id
         |FROM alefdw_stage.staging_pathway_teacher_activity staging
         |         JOIN alefdw.dim_tenant ON dim_tenant.tenant_id = staging.fpta_tenant_id
         |         JOIN alefdw_stage.rel_user te ON te.user_id = staging.fpta_teacher_id
         |         JOIN alefdw_stage.rel_user st ON st.user_id = staging.fpta_student_id
         |         LEFT JOIN alefdw_stage.rel_dw_id_mappings cls ON cls.id = staging.fpta_class_id and cls.entity_type = 'class'
         |         JOIN alefdw_stage.rel_dw_id_mappings crs ON crs.id = staging.fpta_pathway_id and crs.entity_type = 'course'
         |         LEFT JOIN alefdw_stage.rel_dw_id_mappings cac on cac.id = staging.fpta_level_id and cac.entity_type = 'course_activity_container'
         |         LEFT JOIN (select lo_id, lo_dw_id from (
         |            select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
         |                from alefdw.dim_learning_objective
         |              ) t where t.rank = 1
         |            ) AS lesson ON lesson.lo_id = staging.fpta_activity_id
         |         LEFT JOIN alefdw.dim_interim_checkpoint ic on ic.ic_id = staging.fpta_activity_id
         |ORDER BY fpta_dw_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert from select query") {

    val query = transformer.getInsertFromSelectQuery("alefdw", List(1001, 1002, 1003))

    val expectedQuery =
      """
        |INSERT INTO alefdw.fact_pathway_teacher_activity (
        |        fpta_created_time, fpta_dw_created_time, fpta_date_dw_id,
        |        fpta_student_id, fpta_student_dw_id, fpta_class_id, fpta_class_dw_id,
        |        fpta_pathway_id, fpta_pathway_dw_id, fpta_level_id, fpta_level_dw_id,
        |        fpta_tenant_id, fpta_tenant_dw_id, fpta_activity_id, fpta_action_name,
        |        fpta_action_time, fpta_dw_id, fpta_teacher_id, fpta_teacher_dw_id, fpta_activity_dw_id,
        |        fpta_activity_type, fpta_start_date, fpta_end_date, fpta_course_dw_id, fpta_course_activity_container_dw_id,
        |        fpta_activity_progress_status, fpta_activity_type_value, fpta_is_added_as_resource
        |)
        |SELECT fpta_created_time,
        |       fpta_dw_created_time,
        |       fpta_date_dw_id,
        |       fpta_student_id,
        |       st.user_dw_id           AS fpta_student_dw_id,
        |       fpta_class_id,
        |       cls.dw_id               AS fpta_class_dw_id,
        |       fpta_pathway_id,
        |       crs.dw_id                 AS fpta_pathway_dw_id,
        |       fpta_level_id,
        |       cac.dw_id                AS fpta_level_dw_id,
        |       fpta_tenant_id,
        |       dim_tenant.tenant_dw_id AS fpta_tenant_dw_id,
        |       fpta_activity_id,
        |       fpta_action_name,
        |       fpta_action_time,
        |       fpta_dw_id,
        |       fpta_teacher_id,
        |       te.user_dw_id           AS fpta_teacher_dw_id,
        |       NVL(lesson.lo_dw_id, ic.ic_dw_id) AS fpta_activity_dw_id,
        |       CASE WHEN staging.fpta_activity_type IN (1,2) THEN staging.fpta_activity_type
        |           WHEN lesson.lo_id IS NOT NULL THEN 1
        |           WHEN ic.ic_id IS NOT NULL THEN 2
        |        ELSE -1 END AS fpta_activity_type,
        |       fpta_start_date,
        |       fpta_end_date,
        |       crs.dw_id AS fpta_course_dw_id,
        |       cac.dw_id AS fpta_course_activity_container_dw_id,
        |       fpta_activity_progress_status,
        |       fpta_activity_type_value,
        |       fpta_is_added_as_resource
        |FROM alefdw_stage.staging_pathway_teacher_activity staging
        |         JOIN alefdw.dim_tenant ON dim_tenant.tenant_id = staging.fpta_tenant_id
        |         JOIN alefdw_stage.rel_user te ON te.user_id = staging.fpta_teacher_id
        |         JOIN alefdw_stage.rel_user st ON st.user_id = staging.fpta_student_id
        |         LEFT JOIN alefdw_stage.rel_dw_id_mappings cls ON cls.id = staging.fpta_class_id and cls.entity_type = 'class'
        |         JOIN alefdw_stage.rel_dw_id_mappings crs ON crs.id = staging.fpta_pathway_id and crs.entity_type = 'course'
        |         LEFT JOIN alefdw_stage.rel_dw_id_mappings cac on cac.id = staging.fpta_level_id and cac.entity_type = 'course_activity_container'
        |         LEFT JOIN (select lo_id, lo_dw_id from (
        |            select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
        |                from alefdw.dim_learning_objective
        |              ) t where t.rank = 1
        |            ) AS lesson ON lesson.lo_id = staging.fpta_activity_id
        |         LEFT JOIN alefdw.dim_interim_checkpoint ic on ic.ic_id = staging.fpta_activity_id
        |  AND staging.fpta_dw_id IN (1001,1002,1003)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare select statement") {
    val cols = List(
      "fpta_created_time",
      "fpta_dw_created_time",
      "fpta_date_dw_id",
      "fpta_student_id",
      "fpta_student_dw_id",
      "fpta_class_id",
      "fpta_class_dw_id",
      "fpta_pathway_id",
      "fpta_pathway_dw_id",
      "fpta_level_id",
      "fpta_level_dw_id",
      "fpta_tenant_id",
      "fpta_tenant_dw_id",
      "fpta_activity_id",
      "fpta_action_name",
      "fpta_action_time",
      "eventdate",
      "fpta_dw_id",
      "fpta_teacher_id",
      "fpta_teacher_dw_id",
      "fpta_activity_dw_id",
      "fpta_activity_type",
      "fpta_course_dw_id",
      "fpta_course_activity_container_dw_id",
      "fpta_activity_progress_status",
      "fpta_activity_type_value",
      "fpta_is_added_as_resource"
    )
    val expRes =
      s"""fpta_created_time,
         |fpta_dw_created_time,
         |fpta_date_dw_id,
         |fpta_student_id,
         |st.user_dw_id AS fpta_student_dw_id,
         |fpta_class_id,
         |cls.dw_id AS fpta_class_dw_id,
         |fpta_pathway_id,
         |crs.dw_id AS fpta_pathway_dw_id,
         |fpta_level_id,
         |cac.dw_id AS fpta_level_dw_id,
         |fpta_tenant_id,
         |dim_tenant.tenant_dw_id AS fpta_tenant_dw_id,
         |fpta_activity_id,
         |fpta_action_name,
         |fpta_action_time,
         |eventdate,
         |fpta_dw_id,
         |fpta_teacher_id,
         |te.user_dw_id AS fpta_teacher_dw_id,
         |NVL(lesson.lo_dw_id, ic.ic_dw_id) AS fpta_activity_dw_id,
         |CASE WHEN staging.fpta_activity_type IN (1,2) THEN staging.fpta_activity_type
         |           WHEN lesson.lo_id IS NOT NULL THEN 1
         |           WHEN ic.ic_id IS NOT NULL THEN 2
         |        ELSE -1 END AS fpta_activity_type,
         |crs.dw_id AS fpta_course_dw_id,
         |cac.dw_id AS fpta_course_activity_container_dw_id,
         |fpta_activity_progress_status,
         |fpta_activity_type_value,
         |fpta_is_added_as_resource
         |""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    replaceSpecChars(actual) should be(replaceSpecChars(expRes))
  }
}