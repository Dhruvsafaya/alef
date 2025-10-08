package com.alefeducation.warehouse.core

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SqlStringDataTest extends AnyFunSuite with Matchers {

  test("insertSql generates correct SQL statement") {
    val table = "users"
    val sqlData = SqlStringData(
      insertSql = tableName => s"INSERT INTO $tableName (name, age) VALUES (?, ?)",
      startEventsInsert = _ => "",
      idSelectForDelete = "",
      idSelectForUpdate = ""
    )

    sqlData.insertSql(table) shouldEqual "INSERT INTO users (name, age) VALUES (?, ?)"
  }

  test("startEventsInsert generates correct SQL statement") {
    val eventTable = "events"
    val sqlData = SqlStringData(
      insertSql = _ => "",
      startEventsInsert = tableName => s"INSERT INTO $tableName (event, timestamp) VALUES (?, ?)",
      idSelectForDelete = "",
      idSelectForUpdate = ""
    )

    sqlData.startEventsInsert(eventTable) shouldEqual "INSERT INTO events (event, timestamp) VALUES (?, ?)"
  }

  test("idSelectForDelete returns the correct SQL statement") {
    val sqlData = SqlStringData(
      insertSql = _ => "",
      startEventsInsert = _ => "",
      idSelectForDelete = "SELECT id FROM users WHERE status = 'deleted'",
      idSelectForUpdate = ""
    )

    sqlData.idSelectForDelete shouldEqual "SELECT id FROM users WHERE status = 'deleted'"
  }

  test("idSelectForUpdate returns the correct SQL statement") {
    val sqlData = SqlStringData(
      insertSql = _ => "",
      startEventsInsert = _ => "",
      idSelectForDelete = "",
      idSelectForUpdate = "SELECT id FROM users WHERE status = 'pending_update'"
    )

    sqlData.idSelectForUpdate shouldEqual "SELECT id FROM users WHERE status = 'pending_update'"
  }

  test("deleteDanglingSql defaults to null") {
    val sqlData = SqlStringData(
      insertSql = _ => "",
      startEventsInsert = _ => "",
      idSelectForDelete = "",
      idSelectForUpdate = ""
      // Note: Not specifying deleteDanglingSql, so it should take the default value.
    )

    assert(sqlData.deleteDanglingSql == null)
  }
}
