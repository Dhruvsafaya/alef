package com.alefeducation.warehouse.core

import com.alefeducation.util.{Resources, SecretManager}
import com.alefeducation.warehouse.models.WarehouseConnection
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import java.time.Duration

object WarehouseUtils {

  def prepareConnection(secretManager: SecretManager): WarehouseConnection = {
    val schema = Resources.conf.getString("redshift-schema")
    val rcd = secretManager.getRedshift.getOrElse(throw new RuntimeException("Error: redshift.urlkey is required"))
    val database = Resources.conf.getString("redshift.database")
    val connectionURL = s"jdbc:redshift://${rcd.host}:${rcd.port}/$database"
    val conn = WarehouseConnection(schema, connectionURL, "com.amazon.redshift.jdbc.Driver", rcd.username, rcd.password)
    Class.forName(conn.driver)

    val timeOutDurationInMillis = Duration.ofMinutes(Resources.conf.getInt("redshift-connection-timeout-min")).toMillis
    val poolSettings: ConnectionPoolSettings = ConnectionPoolSettings(connectionTimeoutMillis = timeOutDurationInMillis)

    ConnectionPool.singleton(conn.connectionUrl, conn.username, conn.password, poolSettings)
    conn
  }
}
