package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class QuestionPoolAssociationTransformerSpec extends AnyFunSuite with Matchers {

  val transformer: QuestionPoolAssociationTransformer.type = QuestionPoolAssociationTransformer


  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")

    val expectedQuery =
      s"""
         |SELECT rel_question_pool_association_id
         |FROM alefdw_stage.rel_question_pool_association staging
         |  INNER JOIN alefdw.dim_question_pool dqp ON staging.question_pool_uuid = dqp.question_pool_id
         |ORDER BY rel_question_pool_association_id
         |LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }

  test("should prepare insert query") {
    val query = transformer.getInsertFromSelectQuery("alefdw", List(10, 12, 13))
    val expectedQuery =
      """
        |INSERT INTO alefdw.dim_question_pool_association (
        | question_pool_association_created_time,
        | question_pool_association_updated_time,
        | question_pool_association_dw_created_time,
        | question_pool_association_dw_updated_time,
        | question_pool_association_status,
        | question_pool_association_assign_status,
        | question_pool_association_question_code,
        | question_pool_association_question_pool_dw_id,
        | question_pool_association_triggered_by
        |)
        |SELECT
        | question_pool_association_created_time,
        | question_pool_association_updated_time,
        | current_timestamp as question_pool_association_dw_created_time,
        | null as question_pool_association_dw_updated_time,
        | question_pool_association_status,
        | question_pool_association_assign_status,
        | question_pool_association_question_code,
        | dqp.question_pool_dw_id as question_pool_association_question_pool_dw_id,
        | question_pool_association_triggered_by
        |FROM alefdw_stage.rel_question_pool_association staging
        | INNER JOIN alefdw.dim_question_pool dqp ON staging.question_pool_uuid = dqp.question_pool_id
        |WHERE staging.rel_question_pool_association_id IN (10,12,13)
        |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
