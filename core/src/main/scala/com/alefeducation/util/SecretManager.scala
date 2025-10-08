package com.alefeducation.util

import com.alefeducation.schema.secretmanager.{InfluxDBCred, JiraCred, RedshiftConnectionDesc, ServiceDeskCred}

trait SecretManager {

  def init(name: String): Unit

  def loadCertificates(): Unit

  def getKafkaProps: List[(String, String)]

  def getInfluxCred: InfluxDBCred

  def disposeCertificates(): Unit

  def getRedshift: Option[RedshiftConnectionDesc]

  def getServiceDeskSecret: ServiceDeskCred

  def getJiraSecret: JiraCred

  def getRedshiftUrlProp: (String, String)

  def getFabricUrlProp: (String, String)

  def getFabricToken: (String, String)

  def getLaunchDarklySdkKey: Option[String]

  def close(): Unit
}
