package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.{SqlJdbc, SqlStringData}
import com.alefeducation.warehouse.models.WarehouseConnection
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{never, reset, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import scalikejdbc.{AutoSession, DBSession}

class FactSessionTransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  implicit val db = mock[AutoSession]

  val sqlMock = mock[SqlJdbc]

  val cols = List("col1", "col2", "col3")

  val ids: List[Long] = List(2001, 2002, 2003)

  val idsStr = ids.mkString("'", "','", "'")

  val schema = "alefdw"

  val warehouseConnection = WarehouseConnection("rs_schema", "jdbc:redshift://redshift:5439/bigdatadb", "com.amazon.redshift.jdbc.Driver", "usr", "pwd")

  override def afterEach(): Unit = {
    reset(sqlMock)
  }

  object MockFactSessionTransformer extends FactSessionTransformer {

    override val sql: SqlJdbc = sqlMock

    override def prepareSqlStrings(schema: String, factCols: String): SqlStringData = {
      assert(factCols == "col1,\n\tcol2,\n\tcol3")
      assert(schema == "rs_schema")
      SqlStringData(_ => "insertSql", _ => "startEventsInsert", "idSelectForDelete", "idSelectForUpdate", "deleteDanglingSql")
    }

    override val factEntity: String = "practice"
  }

  val transformer: FactSessionTransformer = MockFactSessionTransformer

  test("prepareQueries should prepare and execute queries") {
    val query = "select column_name from information_schema.columns where table_schema = 'rs_schema' and table_name = 'fact_practice_session' and ordinal_position != 1 order by ordinal_position;"
    when(sqlMock.getIds[String](any())(ArgumentMatchers.eq(query))(any[DBSession])).thenReturn(cols)

    transformer.prepareQueries(warehouseConnection)

    verify(sqlMock).getIds[String](any())(ArgumentMatchers.eq(query))(any[DBSession])
    verify(sqlMock, times(3)).localTx(any())
  }

  test("processDanglingEvents should delete dangling") {
    val query = "deleteDanglingSql"

    transformer.processDanglingEvents(query)
    verify(sqlMock).localTx(any())
  }

  test("processDanglingEvents should Not delete dangling") {
    val query = "deleteDanglingSql"

    transformer.processDanglingEvents(null)
    verify(sqlMock, never()).localTx(any())
  }

  test("runQueries should not do anything") {
    transformer.runQueries(warehouseConnection, Nil)

    verify(sqlMock, never()).localTx(any())
  }

  test("processStartEventInLocalTx should process startEvent") {
    val startEvenInsertQuery = s"startEventInsert_$idsStr"
    val startEventInsertFn: String => String = s => {
      assert(s == idsStr)
      startEvenInsertQuery
    }
    val idSelectForUpdate = "idSelectForUpdate"
    val expUpdateQuery = "update alefdw_stage.staging_practice_session set practice_session_is_start_event_processed = true  where practice_session_staging_id in ('2001','2002','2003')"

    when(whenGetIdsByQuery[Long](idSelectForUpdate)).thenReturn(ids)


    transformer.processStartEventInLocalTx(schema, startEventInsertFn, idSelectForUpdate)

    verify(sqlMock).getIds[Long](any())(ArgumentMatchers.eq(idSelectForUpdate))(any[DBSession])
    verify(sqlMock).update(startEvenInsertQuery)
    verify(sqlMock).update(expUpdateQuery)
  }

  test("processStartEventInLocalTx should not do any updates if no events") {
    val idSelectForUpdate = "idSelectForUpdate"
    when(whenGetIdsByQuery[Long](idSelectForUpdate)).thenReturn(Nil)

    transformer.processStartEventInLocalTx(schema, identity, idSelectForUpdate)

    verify(sqlMock).getIds[Long](any())(ArgumentMatchers.eq(idSelectForUpdate))(any[DBSession])
    verify(sqlMock, never()).update(anyString())(any[DBSession])
    verify(sqlMock, never()).update(anyString())(any[DBSession])
  }

  test("processCompletedEventsInLocalTx should process completedEvent") {
    val idsString = ids.flatMap(x => List(x, x)).mkString("'", "','", "'")
    val insertSqlQuery = s"startEventInsert_$idsString"
    val insertSqlFn: String => String = s => {
      assert(s == idsString)
      insertSqlQuery
    }
    val idSelectForDelete = "idSelectForDelete"
    val deleteQuery = "delete from alefdw_stage.staging_practice_session  where practice_session_staging_id in ('2001','2001','2002','2002','2003','2003')"
    when(whenGetIdsByQuery[(Long, Long)](idSelectForDelete)).thenReturn(ids.map(x => (x, x)))

    transformer.processCompletedEventsInLocalTx(schema, insertSqlFn, idSelectForDelete)

    verify(sqlMock).getIds[(Long, Long)](any())(ArgumentMatchers.eq(idSelectForDelete))(any[DBSession])
    verify(sqlMock).update(insertSqlQuery)
    verify(sqlMock).update(deleteQuery)
  }

  test("processCompletedEventsInLocalTx should not do any updates if no events") {
    val idSelectForDelete = "idSelectForDelete"
    when(whenGetIdsByQuery[(Long, Long)](idSelectForDelete)).thenReturn(Nil)

    transformer.processCompletedEventsInLocalTx(schema, identity, idSelectForDelete)

    verify(sqlMock).getIds[(Long, Long)](any())(ArgumentMatchers.eq(idSelectForDelete))(any[DBSession])
    verify(sqlMock, never()).update(anyString())(any[DBSession])
    verify(sqlMock, never()).update(anyString())(any[DBSession])
  }

  test("should return empty maps") {
    assert(transformer.tableNotation == Map.empty)
    assert(transformer.columnNotation == Map.empty)
    assert(transformer.pkNotation == Map.empty)
  }

  private def whenGetIdsByQuery[A](query: String) = {
    sqlMock.getIds[A](any())(ArgumentMatchers.eq(query))(any[DBSession])
  }
}
