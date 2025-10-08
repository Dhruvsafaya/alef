package com.alefeducation.warehouse.core

import com.alefeducation.schema.secretmanager.RedshiftConnectionDesc
import com.alefeducation.util.SecretManager
import com.alefeducation.warehouse.models.WarehouseConnection
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class WarehouseUtilsTest extends AnyFunSuite with Matchers {

  val mockSM: SecretManager = mock[SecretManager]

  test("prepareConnection should return WarehouseConnection") {
    when(mockSM.getRedshift).thenReturn(Some(RedshiftConnectionDesc("username", "password", "engine", "host", 5413, "clusterId")))

    val wc = WarehouseUtils.prepareConnection(mockSM)

    assert(wc == WarehouseConnection("rs_schema", "jdbc:redshift://host:5413/bigdatadb", "com.amazon.redshift.jdbc.Driver", "username", "password"))
  }

}