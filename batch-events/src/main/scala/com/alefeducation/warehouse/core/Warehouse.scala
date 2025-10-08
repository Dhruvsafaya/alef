package com.alefeducation.warehouse.core

import com.alefeducation.util.DataFrameUtility.handleSqlException
import com.alefeducation.util.Resources.{conf, getSqlExceptionAttempts}
import com.alefeducation.util.{AWSSecretManager, FeatureService, SecretManager}
import com.alefeducation.warehouse.core.WarehouseUtils.prepareConnection
import com.alefeducation.warehouse.models.WarehouseConnection
import org.apache.log4j.Logger
import scalikejdbc.AutoSession

trait Warehouse[A] {

  val log = Logger.getLogger(classOf[Warehouse[A]])
  val DAY_THRESHOLD = conf.getString("query-day-threshold")

  var featureService: Option[FeatureService] = None

  def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[A]

  def runQueries(connection: WarehouseConnection, queryMeta: List[A])(implicit session: AutoSession): Unit = {
    // for overriding in transformers
  }

  def run(secretManager: SecretManager, fn: SecretManager => Option[FeatureService]): Unit = {
    secretManager.init("transformers")
    featureService = fn(secretManager)
    val connection = prepareConnection(secretManager)
    implicit val autoSession: AutoSession = AutoSession
    handleSqlException({
      val queryMetas = prepareQueries(connection)
      runQueries(connection, queryMetas)
    }, getSqlExceptionAttempts())
    secretManager.close()
  }

  def main(args: Array[String]): Unit = {
    run(AWSSecretManager, FeatureService(_))
  }
}
