package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class AcademicYearTransformerTest extends AnyFunSuite with Matchers {

  val transformer: AcademicYearTransformer.type = AcademicYearTransformer
  val connection: WarehouseConnection = WarehouseConnection(
    "alefdw", "http://localhost:8080", "jdbc", "username", "password"
  )

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession

    val query = transformer.prepareQueries(connection)
    val stagingTableName = "rel_academic_year"
    val expectedQuery =
      s"""
         |SELECT
         |  academic_year_delta_dw_id
         |FROM alefdw_stage.rel_academic_year staging
         |  LEFT OUTER JOIN alefdw_stage.rel_user cb ON cb.user_id = staging.academic_year_created_by
         |  LEFT OUTER JOIN alefdw_stage.rel_user ub ON ub.user_id = staging.academic_year_updated_by
         |  LEFT OUTER JOIN alefdw.dim_organization org ON org.organization_code = staging.academic_year_organization_code
         |  LEFT OUTER JOIN alefdw.dim_school ON dim_school.school_id = staging.academic_year_school_id
         |WHERE ((staging.academic_year_organization_code IS NULL AND org.organization_code IS NULL) OR
         |        (staging.academic_year_organization_code IS NOT NULL AND org.organization_code IS NOT NULL))
         |      AND ((staging.academic_year_school_id IS NULL AND dim_school.school_id IS NULL) OR
         |        (staging.academic_year_school_id IS NOT NULL AND dim_school.school_id IS NOT NULL))
         |ORDER BY academic_year_delta_dw_id
         |LIMIT 60000
         |""".stripMargin

    query.head.stagingTable should be(stagingTableName)
    replaceSpecChars(query.head.selectSQL) shouldBe replaceSpecChars(expectedQuery)
  }


  test("should prepare select statement") {
    val cols = List(
      "academic_year_delta_dw_id",
      "academic_year_created_time",
      "academic_year_updated_time",
      "academic_year_deleted_time",
      "academic_year_dw_created_time",
      "academic_year_dw_updated_time",
      "academic_year_status",
      "academic_year_state",
      "academic_year_id",
      "academic_year_school_id",
      "academic_year_school_dw_id",
      "academic_year_start_date",
      "academic_year_end_date",
      "academic_year_is_roll_over_completed",
      "academic_year_organization_code",
      "academic_year_organization_dw_id",
      "academic_year_created_by",
      "academic_year_created_by_dw_id",
      "academic_year_updated_by",
      "academic_year_updated_by_dw_id"
    )
    val expRes =
      s"""
         |academic_year_delta_dw_id,
         |academic_year_created_time,
         |academic_year_updated_time,
         |academic_year_deleted_time,
         |academic_year_dw_created_time,
         |academic_year_dw_updated_time,
         |academic_year_status,
         |academic_year_state,
         |academic_year_id,
         |academic_year_school_id,
         |dim_school.school_dw_id AS academic_year_school_dw_id,
         |academic_year_start_date,
         |academic_year_end_date,
         |academic_year_is_roll_over_completed,
         |academic_year_organization_code,
         |org.organization_dw_id AS academic_year_organization_dw_id,
         |academic_year_created_by,
         |cb.user_dw_id AS academic_year_created_by_dw_id,
         |academic_year_updated_by,
         |ub.user_dw_id AS academic_year_updated_by_dw_id
         |""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    replaceSpecChars(actual) should be(replaceSpecChars(expRes))
  }
}
