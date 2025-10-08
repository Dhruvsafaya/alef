package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class AssignmentInstanceTransformerTest extends AnyFunSuite with Matchers {

  val transformer = AssignmentInstanceTransformer
  val schema = "alefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(schema)
    val expectedSelectQuery =
      """
         |SELECT a.assignment_instance_staging_id
         |FROM alefdw_stage.rel_assignment_instance a
         |         INNER JOIN alefdw_stage.rel_user teacher ON teacher.user_id = a.teacher_uuid
         |         INNER JOIN alefdw.dim_tenant tnt ON tnt.tenant_id = a.tenant_uuid
         |         INNER JOIN alefdw.dim_assignment ai on ai.assignment_id = a.assignment_uuid
         |         INNER JOIN alefdw.dim_grade g ON g.grade_id = a.grade_uuid
         |         LEFT JOIN alefdw.dim_subject s ON s.subject_id = a.subject_uuid
         |         LEFT JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = a.class_uuid AND c.entity_type = 'class'
         |         INNER JOIN (select lo_id, lo_dw_id from (
         |                  select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
         |                  from alefdw.dim_learning_objective
         |                   ) t where t.rank = 1
         |                ) AS lo ON lo.lo_id = a.lo_uuid
         |         LEFT JOIN alefdw.dim_section sec on sec.section_id = a.section_uuid
         |WHERE ((a.section_uuid is null) OR (sec.section_id is not null))
         |  AND ((a.subject_uuid is null) OR (s.subject_id is not null))
         |  AND ((a.class_uuid is null) OR (c.id is not null))
         |ORDER BY a.assignment_instance_staging_id
         |LIMIT 60000
         |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    val expectedInsertQuery =
      """INSERT INTO alefdw.dim_assignment_instance
        |( assignment_instance_id, assignment_instance_created_time, assignment_instance_updated_time,
        | assignment_instance_deleted_time, assignment_instance_dw_created_time, assignment_instance_dw_updated_time,
        |  assignment_instance_instructional_plan_id, assignment_instance_due_on,
        |  assignment_instance_allow_late_submission, assignment_instance_type,
        |  assignment_instance_start_on, assignment_instance_status, assignment_instance_trimester_id,
        |  assignment_instance_teaching_period_id, assignment_instance_assignment_dw_id,
        |  assignment_instance_teacher_dw_id, assignment_instance_grade_dw_id, assignment_instance_subject_dw_id,
        |  assignment_instance_class_dw_id, assignment_instance_lo_dw_id, assignment_instance_section_dw_id,
        |  assignment_instance_tenant_dw_id )
        |  SELECT assignment_instance_id, assignment_instance_created_time, assignment_instance_updated_time,
        |  assignment_instance_deleted_time, assignment_instance_dw_created_time, assignment_instance_dw_updated_time,
        |  assignment_instance_instructional_plan_id, assignment_instance_due_on,
        |  assignment_instance_allow_late_submission, assignment_instance_type, assignment_instance_start_on,
        |  assignment_instance_status, assignment_instance_trimester_id, assignment_instance_teaching_period_id,
        |  ai.assignment_dw_id as assignment_instance_assignment_dw_id,
        |  teacher.user_dw_id as assignment_instance_teacher_dw_id, g.grade_dw_id as assignment_instance_grade_dw_id,
        |  s.subject_dw_id as assignment_instance_subject_dw_id, c.dw_id as assignment_instance_class_dw_id,
        |  lo.lo_dw_id as assignment_instance_lo_dw_id, sec.section_dw_id as assignment_instance_section_dw_id,
        |  tnt.tenant_dw_id as assignment_instance_tenant_dw_id FROM alefdw_stage.rel_assignment_instance a INNER JOIN
        |  alefdw_stage.rel_user teacher ON teacher.user_id = a.teacher_uuid INNER JOIN
        |  alefdw.dim_tenant tnt ON tnt.tenant_id = a.tenant_uuid
        |  INNER JOIN alefdw.dim_assignment ai on ai.assignment_id = a.assignment_uuid
        |  INNER JOIN alefdw.dim_grade g ON g.grade_id = a.grade_uuid
        |  LEFT JOIN alefdw.dim_subject s ON s.subject_id = a.subject_uuid
        |  LEFT JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = a.class_uuid AND c.entity_type = 'class'
        |  INNER JOIN (select lo_id, lo_dw_id from ( select lo_id, lo_dw_id, row_number() over
        |  (partition by lo_id order by lo_created_time desc) as rank from alefdw.dim_learning_objective ) t where t.rank = 1 )
        |  AS lo ON lo.lo_id = a.lo_uuid LEFT JOIN alefdw.dim_section sec on sec.section_id = a.section_uuid WHERE
        |  ((a.section_uuid is null) OR (sec.section_id is not null)) AND ((a.subject_uuid is null) OR (s.subject_id is not null))
        |  AND ((a.class_uuid is null) OR (c.id is not null)) AND a.assignment_instance_staging_id IN (3003,3004,3005)""".stripMargin
    transformer.getPkColumn() should be("assignment_instance_staging_id")
    transformer.getStagingTableName() should be("rel_assignment_instance")
    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))
    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
