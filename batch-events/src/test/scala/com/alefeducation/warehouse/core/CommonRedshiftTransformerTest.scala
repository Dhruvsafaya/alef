package com.alefeducation.warehouse.core

import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CommonRedshiftTransformerTest extends AnyFunSuite with Matchers {

  case class MockRedshiftTransformer() extends CommonRedshiftTransformer {
    override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = ""

    override val pkCol: List[String] = List("Id")
    override val mainColumnNames: List[String] = List("C1","C2","C3")
    override val stagingTableName: String = "TableName"
    override val mainTableName: String = "MainTableName"

    override def pkNotation: Map[String, String] = Map("rel_student" -> "rel_student_id")
  }

  test("should prepare queries") {
    implicit val autoSession: AutoSession = AutoSession

    val insertStatement =
      s"""
         |
         |INSERT INTO Schema.MainTableName
         | (C1, \nC2, \nC3)
         | (
         |  
         | )
         |""".stripMargin


    val expectedVal = List(
      QueryMeta(
        MockRedshiftTransformer().stagingTableName,"",insertStatement
      )
    )

    MockRedshiftTransformer().prepareQueries(WarehouseConnection("Schema", "connectionUrl", "driver", "username", "password")) should be (expectedVal)

  }
}
