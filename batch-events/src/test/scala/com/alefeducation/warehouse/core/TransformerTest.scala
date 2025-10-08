package com.alefeducation.warehouse.core

import com.alefeducation.schema.secretmanager.RedshiftConnectionDesc
import com.alefeducation.util.{FeatureService, SecretManager}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import scalikejdbc.DBSession

class TransformerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  implicit val db: DBSession = mock[DBSession]

  val Ids: List[Long] = List(1001, 1002, 1003)

  val sqlMock: SqlJdbc = mock[SqlJdbc]

  val defaultSchema: String = "rs_schema"

  override def afterEach(): Unit = {
    reset(sqlMock)
  }

  class MockTransformer extends Transformer {

    override val sql: SqlJdbc = sqlMock

    override def getSelectQuery(schema: String): String = {
      assert(schema == defaultSchema)
      "selectQuery"
    }

    override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
      assert(schema == defaultSchema)
      s"insertFromSelectQuery(${ids.mkString(",")})"
    }

    override def getPkColumn(): String = "pkColumn"

    override def getStagingTableName(): String = "stagingTable"

  }

  val transformer = new MockTransformer()

  test("should execute selectIds and return ids list") {
    val query = transformer.getSelectQuery(defaultSchema)
    when(getIdsForWhen(query)).thenReturn(Ids)

    assert(transformer.selectIds(defaultSchema) == List(1001, 1002, 1003))

    verifyGetIds(verify(sqlMock), query)
  }

  test("should execute insertFromSelect") {
    transformer.insertFromSelect(defaultSchema, Ids)

    verify(sqlMock).update("insertFromSelectQuery(1001,1002,1003)")
  }

  test("should execute delete") {
    transformer.delete(defaultSchema, Ids)

    verify(sqlMock).update("DELETE FROM rs_schema_stage.stagingTable WHERE pkColumn IN (1001,1002,1003)")
  }

  test("should execute runTransformer when select return empty ids") {
    val query = transformer.getSelectQuery(defaultSchema)
    when(getIdsForWhen(query)).thenReturn(Nil)

    transformer.runTransformer(defaultSchema)

    verifyGetIds(verify(sqlMock), query)
    verify(sqlMock, never()).update(anyString())(any[DBSession])
  }

  test("should execute runTransformer") {
    val query = transformer.getSelectQuery(defaultSchema)
    when(getIdsForWhen(query)).thenReturn(Ids).thenReturn(Nil)

    transformer.runTransformer(defaultSchema)

    verifyGetIds(verify(sqlMock, times(2)), query)
    verify(sqlMock, times(1)).localTx(any())
  }

  test("should execute insertFromSelectThenDelete") {
    transformer.insertFromSelectThenDelete(defaultSchema, Ids)

    verify(sqlMock).update("insertFromSelectQuery(1001,1002,1003)")
    verify(sqlMock).update("DELETE FROM rs_schema_stage.stagingTable WHERE pkColumn IN (1001,1002,1003)")
  }

  test("should execute run") {
    val secretManager: SecretManager = mock[SecretManager]
    when(secretManager.getRedshift).thenReturn(Some(RedshiftConnectionDesc("usr1", "pwd2", "engn3", "redshift", 5455, "cldId55")))
    val query = transformer.getSelectQuery(defaultSchema)
    when(getIdsForWhen(query)).thenReturn(Ids).thenReturn(Nil)

    transformer.run(secretManager, _ => None)

    verify(secretManager).init("transformers")
    verify(secretManager).close()
  }

  private def verifyGetIds(sql: SqlJdbc, query: String): Unit = {
    sql.getIds(any())(ArgumentMatchers.eq(query))(any[DBSession])
  }

  private def getIdsForWhen(query: String) = {
    sqlMock.getIds[Long](any())(ArgumentMatchers.eq(query))(any[DBSession])
  }
}
