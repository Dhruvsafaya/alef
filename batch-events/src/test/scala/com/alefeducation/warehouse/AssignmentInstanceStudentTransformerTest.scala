package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AssignmentInstanceStudentTransformerTest extends AnyFunSuite with Matchers {

  val transformer = AssignmentInstanceStudentTransformer
  val schema = "alefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(schema)
    val expectedSelectQuery =
      """
        |SELECT
        |     a.rel_ais_id
        |FROM alefdw_stage.rel_assignment_instance_student a
        |INNER JOIN alefdw.dim_assignment_instance ins on ins.assignment_instance_id=a.ais_instance_id
        |INNER JOIN alefdw_stage.rel_user std on std.user_id=a.ais_student_id
        |ORDER BY a.rel_ais_id
        |      LIMIT 60000
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(5003, 5004, 5005))

    val expectedInsertQuery =
      """
        |INSERT INTO alefdw.dim_assignment_instance_student (
        |	ais_created_time,
        |	ais_updated_time,
        |	ais_deleted_time,
        |	ais_dw_created_time,
        |	ais_dw_updated_time,
        |	ais_status,
        |	ais_instance_dw_id,
        |	ais_student_dw_id
        |)
        | SELECT
        |	ais_created_time,
        |	ais_updated_time,
        |	ais_deleted_time,
        |	ais_dw_created_time,
        |	ais_dw_updated_time,
        |	ais_status,
        |	ins.assignment_instance_dw_id as ais_instance_dw_id,
        |	std.user_dw_id as ais_student_dw_id
        |FROM alefdw_stage.rel_assignment_instance_student a
        |INNER JOIN alefdw.dim_assignment_instance ins on ins.assignment_instance_id=a.ais_instance_id
        |INNER JOIN alefdw_stage.rel_user std on std.user_id=a.ais_student_id
        | AND a.rel_ais_id IN (5003,5004,5005)
        |""".stripMargin

    transformer.getPkColumn() should be("rel_ais_id")
    transformer.getStagingTableName() should be("rel_assignment_instance_student")
    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))
    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
