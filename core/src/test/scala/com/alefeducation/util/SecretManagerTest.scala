package com.alefeducation.util

import com.alefeducation.schema.secretmanager.{InfluxDBCred, RedshiftConnectionDesc, ServiceDeskCred}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, reset, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

class SecretManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  val secretManager: AWSSecretManager.type = AWSSecretManager
  val mockClient: SecretsManagerClient = mock(classOf[SecretsManagerClient])

  override def afterEach(): Unit = {
    reset(mockClient)
  }

  test("init should initialise required variables") {
    val testJobName = "testJobName"

    secretManager.init(testJobName)

    assert(secretManager.jobName == testJobName)
    assert(secretManager.client != null)
  }


  test("getInfluxCred should get InfluxDBCred data") {
    secretManager.client = mockClient
    val responseJson = """{"user": "user_influx", "pass": "password_influx"}"""
    mockResponse(responseJson)

    assert(secretManager.getInfluxCred == InfluxDBCred("user_influx", "password_influx"))
  }

  test("getRedshift should get RedshiftConnectionDesc") {
    secretManager.client = mockClient
    val responseJson =
      """
        |{
        | "username": "rs_username",
        | "password": "rs_password",
        | "engine": "rs_engine",
        | "host": "rs_host",
        | "port": 5439,
        | "dbClusterIdentifier": "rs_cluster_id"
        |}
        |""".stripMargin
    mockResponse(responseJson)

    assert(secretManager.getRedshift.contains(RedshiftConnectionDesc("rs_username", "rs_password", "rs_engine", "rs_host", 5439, "rs_cluster_id")))
  }

  test("getRedshiftUrlProp should get redshift connection's url") {
    secretManager.client = mockClient
    val responseJson =
      """
        |{
        | "username": "rs_username",
        | "password": "rs_password",
        | "engine": "rs_engine",
        | "host": "rs_host",
        | "port": 5439,
        | "dbClusterIdentifier": "rs_cluster_id"
        |}
        |""".stripMargin
    mockResponse(responseJson)

    assert(secretManager.getRedshiftUrlProp == ("url", "jdbc:redshift://rs_host:5439/bigdatadb?user=rs_username&password=rs_password&LoginTimeout=300"))
  }

  test("getServiceDeskSecret should get ServiceDeskCred") {
    secretManager.client = mockClient
    val responseJson =
      """
        |{
        | "refresh_token": "token",
        | "client_id": "clientId",
        | "client_secret": "clientSecret"
        |}
        |""".stripMargin
    mockResponse(responseJson)

    assert(secretManager.getServiceDeskSecret == ServiceDeskCred("token", "clientId", "clientSecret"))
  }

  private def mockResponse(responseJson: String) = {
    val secretValueRequest: GetSecretValueRequest = any[GetSecretValueRequest]
    val response = GetSecretValueResponse.builder().secretString(responseJson).build()
    when(mockClient.getSecretValue(secretValueRequest)).thenReturn(response)
  }
}
