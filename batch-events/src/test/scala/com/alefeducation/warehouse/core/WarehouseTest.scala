package com.alefeducation.warehouse.core

import com.alefeducation.schema.secretmanager.RedshiftConnectionDesc
import com.alefeducation.util.{FeatureService, SecretManager}
import com.alefeducation.warehouse.models.WarehouseConnection
import org.mockito.Mockito.{verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import scalikejdbc.AutoSession

class WarehouseTest extends AnyFunSuite with Matchers {

  implicit val session: AutoSession = AutoSession

  val mockSM: SecretManager = mock[SecretManager]
  val mockFS: FeatureService = mock[FeatureService]

  val expLst = List("one", "two", "three")

  val warahouseConnection = WarehouseConnection("rs_schema", "jdbc:redshift://redshift:5439/bigdatadb", "com.amazon.redshift.jdbc.Driver", "usr", "pwd")

  val warehouse = new Warehouse[String] {

    override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[String] = {
      expLst
    }

    override def runQueries(connection: WarehouseConnection, queryMeta: List[String])(implicit session: AutoSession): Unit = {
      assert(expLst == queryMeta)
      assert(connection == warahouseConnection)
    }
  }

  test("run should execute transformer stages") {
    when(mockSM.getRedshift).thenReturn(Some(RedshiftConnectionDesc("usr", "pwd", "engn", "redshift", 5439, "clusterId123")))

    warehouse.run(mockSM, _ => Some(mockFS))

    verify(mockSM).init("transformers")
    verify(mockSM).getRedshift
    verify(mockSM).close()
  }

  test("prepareQueries should return List of queries") {
    assert(warehouse.prepareQueries(warahouseConnection) == expLst)
  }

  test("runQueries should execute queries") {
    warehouse.runQueries(warahouseConnection, expLst)
  }

}
