package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class TagTransformerTest  extends AnyFunSuite with Matchers {

  val transformer: TagTransformer.type = TagTransformer
  val connection: WarehouseConnection = WarehouseConnection("alefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""
         |SELECT
         |   	tag_dw_id
         |FROM alefdw_stage.rel_tag staging
         |LEFT JOIN alefdw.dim_school s ON staging.tag_association_id = s.school_id AND tag_type = 'SCHOOL'
         |LEFT JOIN alefdw.dim_grade g ON staging.tag_association_id = g.grade_id AND tag_type = 'GRADE'
         |WHERE s.school_dw_id IS NOT NULL or g.grade_dw_id IS NOT NULL
         |ORDER BY tag_dw_id
         |LIMIT 60000
         |""".stripMargin

    queryMetas.head.stagingTable should be("rel_tag")
    replaceSpecChars(queryMetas.head.selectSQL.stripMargin) should be(replaceSpecChars(expectedSelectStatement))
  }

  test("should prepare select statement") {
    val cols = List(
      "tag_association_dw_id",
      "any_col"
    )
    val expRes =
      """
        |nvl(s.school_dw_id, g.grade_dw_id) AS tag_association_dw_id,
        |any_col
        |""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    replaceSpecChars(actual) should be(replaceSpecChars(expRes))
  }

}
