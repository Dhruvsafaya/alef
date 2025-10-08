package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class InstructionalPlanTransformerTest extends AnyFunSuite with Matchers {

  val transformer: InstructionalPlanTransformer.type = InstructionalPlanTransformer
  val connection: WarehouseConnection = WarehouseConnection(
    "testalefdw",
    "http://localhost:8080",
    "jdbc",
    "username",
    "password"
  )

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession

    val stagingTableName: String = "rel_instructional_plan"

    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |  rel_instructional_plan_id
         |FROM testalefdw_stage.$stagingTableName ip
         |  LEFT JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = ip.lo_uuid
         |  INNER JOIN testalefdw.dim_week w ON w.week_id = ip.week_uuid
         |  LEFT JOIN testalefdw.dim_interim_checkpoint ic ON ic.ic_id = ip.ic_uuid
         |  INNER JOIN testalefdw.dim_content_repository cr ON cr.content_repository_id = ip.content_repository_uuid
         |WHERE ((ic_uuid IS NULL AND ic_dw_id IS NULL) OR (ic_uuid IS NOT NULL AND ic_dw_id IS NOT NULL))
         |  AND ((lo_uuid IS NULL AND lo_dw_id IS NULL) OR (lo_uuid IS NOT NULL AND lo_dw_id IS NOT NULL))
         |
         |ORDER BY rel_instructional_plan_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be(stagingTableName)
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "instructional_plan_created_time",
      "instructional_plan_updated_time",
      "instructional_plan_deleted_time",
      "instructional_plan_dw_created_time",
      "instructional_plan_dw_updated_time",
      "instructional_plan_status",
      "instructional_plan_id",
      "instructional_plan_name",
      "instructional_plan_curriculum_id",
      "instructional_plan_curriculum_subject_id",
      "instructional_plan_curriculum_grade_id",
      "instructional_plan_content_academic_year_id",
      "instructional_plan_item_order",
      "instructional_plan_item_week_dw_id",
      "instructional_plan_item_lo_dw_id",
      "instructional_plan_item_ccl_lo_id",
      "instructional_plan_item_optional",
      "instructional_plan_item_instructor_led",
      "instructional_plan_item_default_locked",
      "instructional_plan_item_type",
      "instructional_plan_item_ic_dw_id",
      "instructional_plan_content_repository_id",
      "instructional_plan_content_repository_dw_id"
    )
    val expRes =
      """instructional_plan_created_time,
        |instructional_plan_updated_time,
        |instructional_plan_deleted_time,
        |instructional_plan_dw_created_time,
        |instructional_plan_dw_updated_time,
        |instructional_plan_status,
        |instructional_plan_id,
        |instructional_plan_name,
        |instructional_plan_curriculum_id,
        |instructional_plan_curriculum_subject_id,
        |instructional_plan_curriculum_grade_id,
        |instructional_plan_content_academic_year_id,
        |instructional_plan_item_order,
        |w.week_dw_id AS instructional_plan_item_week_dw_id,
        |lo.lo_dw_id AS instructional_plan_item_lo_dw_id,
        |instructional_plan_item_ccl_lo_id,
        |instructional_plan_item_optional,
        |instructional_plan_item_instructor_led,
        |instructional_plan_item_default_locked,
        |instructional_plan_item_type,
        |ic.ic_dw_id AS instructional_plan_item_ic_dw_id,
        |instructional_plan_content_repository_id,
        |cr.content_repository_dw_id AS instructional_plan_content_repository_dw_id""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
