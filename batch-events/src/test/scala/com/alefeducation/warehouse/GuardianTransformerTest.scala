package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class GuardianTransformerTest  extends AnyFunSuite with Matchers {

  val transformer: GuardianTransformer.type = GuardianTransformer
  val connection: WarehouseConnection = WarehouseConnection(
    "testalefdw", "http://localhost:8080", "jdbc", "username", "password"
  )


  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession

    val stagingTableName: String = "rel_guardian"

    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |  rel_guardian_dw_id
         |FROM testalefdw_stage.$stagingTableName
         |  JOIN testalefdw_stage.rel_user ON rel_user.user_id = rel_guardian.guardian_id
         |  LEFT JOIN testalefdw_stage.rel_user alias_student ON alias_student.user_id = rel_guardian.student_id
         |WHERE (student_id IS NULL AND alias_student.user_dw_id IS NULL)
         |  OR (student_id IS NOT NULL AND alias_student.user_dw_id IS NOT NULL)
         |
         |ORDER BY rel_guardian_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be(stagingTableName)
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "rel_guardian_dw_id",
      "guardian_created_time",
      "guardian_updated_time",
      "guardian_deleted_time",
      "guardian_dw_created_time",
      "guardian_dw_updated_time",
      "guardian_active_until",
      "guardian_status",
      "guardian_id",
      "guardian_dw_id",
      "guardian_student_dw_id",
      "guardian_invitation_status",
    )
    val expRes =
      """rel_guardian_dw_id,
        |guardian_created_time,
        |guardian_updated_time,
        |guardian_deleted_time,
        |guardian_dw_created_time,
        |guardian_dw_updated_time,
        |guardian_active_until,
        |guardian_status,
        |guardian_id,
        |rel_user.user_dw_id AS guardian_dw_id,
        |alias_student.user_dw_id AS guardian_student_dw_id,
        |guardian_invitation_status""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
