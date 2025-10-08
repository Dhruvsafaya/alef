package com.alefeducation.warehouse.core

import com.alefeducation.util.DataFrameUtility.handleSqlException
import com.alefeducation.util.Resources.{conf, getSqlExceptionAttempts}
import com.alefeducation.util.{AWSSecretManager, FeatureService, SecretManager}
import com.alefeducation.warehouse.core.WarehouseUtils.prepareConnection
import org.apache.log4j.Logger
import scalikejdbc.{AutoSession, DBSession}

import scala.annotation.tailrec

/**
 * For create new transformer please use CommonTransformer abstract class
 */
trait Transformer {

  val log = Logger.getLogger(classOf[Transformer])
  val QUERY_LIMIT = conf.getString("query-limit")

  var featureService: Option[FeatureService] = None

  val sql: SqlJdbc

  def getSelectQuery(schema: String): String

  def getInsertFromSelectQuery(schema: String, ids: List[Long]): String

  def getPkColumn(): String

  def getStagingTableName(): String

  private[core] def selectIds(schema: String)(implicit session: DBSession): List[Long] = {
    val query = getSelectQuery(schema)
    log.info(s"executing select query: $query")
    sql.getIds(_.long(getPkColumn()))(query)
  }

  private[core] def insertFromSelect(schema: String, ids: List[Long])(implicit session: DBSession): Unit = {
    val query = getInsertFromSelectQuery(schema, ids)
    log.info(s"executing insert from select query: $query")
    sql.update(query)
  }

  private[core] def delete(schema: String, ids: List[Long])(implicit session: DBSession): Unit = {
    val idClause = ids.mkString(",")
    val query = s"DELETE FROM ${schema}_stage.${getStagingTableName()} WHERE ${getPkColumn()} IN ($idClause)"
    log.info(s"executing delete query: $query")
    sql.update(query)
  }

  private[core] def runTransformer(schema: String)(implicit session: DBSession): Unit = {
    @tailrec
    def loop(schema: String): Unit = {
      val ids = selectIds(schema)

      if (ids.nonEmpty) {
        sql.localTx { implicit session =>
          insertFromSelectThenDelete(schema, ids)
        }
        loop(schema)
      }
    }

    loop(schema)
  }

  private[core] def run(secretManager: SecretManager, fn: SecretManager => Option[FeatureService])(implicit db: DBSession): Unit = {
    secretManager.init("transformers")
    featureService = fn(secretManager)
    val connection = prepareConnection(secretManager)
    try {
      handleSqlException(runTransformer(connection.schema), getSqlExceptionAttempts())
    } finally {
      secretManager.close()
    }
  }

  private[core] def insertFromSelectThenDelete(schema: String, ids: List[Long])(implicit db: DBSession): Unit = {
    insertFromSelect(schema, ids)
    delete(schema, ids)
  }

  def main(args: Array[String]): Unit = {
    implicit val autoSession: AutoSession = AutoSession
    val secretManager: SecretManager = AWSSecretManager
    run(secretManager, FeatureService(_))
  }
}
