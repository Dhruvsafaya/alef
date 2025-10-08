package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.{SqlJdbc, SqlStringData, WarehouseTransformer}
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.{AutoSession, DBSession, SQL}

trait FactSessionTransformer extends WarehouseTransformer {

  override def tableNotation: Map[String, String] = Map()

  override def columnNotation: Map[String, String] = Map()

  override def pkNotation: Map[String, String] = Map()

  val sql: SqlJdbc

  def prepareSqlStrings(schema: String, factCols: String): SqlStringData

  val factEntity: String

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val query = s"select column_name from information_schema.columns where table_schema = '${connection.schema}' and table_name = 'fact_${factEntity}_session' and ordinal_position != 1 order by ordinal_position;"
    val factCols = sql.getIds(_.string("column_name"))(query).mkString(",\n\t")

    val factData = prepareSqlStrings(connection.schema, factCols)
    processEvents(connection.schema, factData)

    List.empty
  }

  def processEvents(schema: String, factData: SqlStringData): Unit = {
    processCompletedEvents(schema, factData.insertSql, factData.idSelectForDelete)
    processStartEvents(schema, factData.startEventsInsert, factData.idSelectForUpdate)
    processDanglingEvents(factData.deleteDanglingSql)
  }

  def processStartEvents(schema: String, startEventsInsert: String => String, idSelectForUpdate: String): Unit = {
    sql localTx { implicit session =>
      processStartEventInLocalTx(schema, startEventsInsert, idSelectForUpdate)
    }
  }

  def processStartEventInLocalTx(schema: String, startEventsInsert: String => String, idSelectForUpdate: String)(implicit db: DBSession): Unit = {
    val startTime = System.currentTimeMillis()
    var insertedRecordsCount = 0
    var updatedRecordsCount = 0
    val updateIds = sql.getIds(_.long("start_id"))(idSelectForUpdate)
    val idsClause = updateIds.mkString("'", "','", "'")
    if (updateIds.nonEmpty) {
      val query = startEventsInsert(idsClause)
      log.info(query)
      insertedRecordsCount = sql.update(query)
      val updateSql = s"update ${schema}_stage.staging_${factEntity}_session " +
        s"set ${factEntity}_session_is_start_event_processed = true " +
        s" where ${factEntity}_session_staging_id in ($idsClause)"
      updatedRecordsCount = sql.update(updateSql)
    }
    log.info(s"InProgress $factEntity events put in ${factEntity}_fact and marked as processed in staging_fact")
    log.info(s"Records affected count: inserted=$insertedRecordsCount, updated=${updatedRecordsCount}")
    log.info(s"It took: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
  }

  def processCompletedEvents(schema: String, insertSql: String => String, idSelectForDelete: String): Unit = {
    sql localTx { implicit session =>
      processCompletedEventsInLocalTx(schema, insertSql, idSelectForDelete)
    }
  }

  def processCompletedEventsInLocalTx(schema: String, insertSql: String => String, idSelectForDelete: String)(implicit db: DBSession): Unit = {
    log.info("Starting Transaction")
    val startTime = System.currentTimeMillis()
    var insertedRecordsCount = 0
    var deletedRecordsCount = 0
    val deleteIds = sql.getIds(x => (x.long("start_id"), x.long("end_id")))(idSelectForDelete).flatMap(t => List(t._1, t._2))
    if (deleteIds.nonEmpty) {
      val idsClause = deleteIds.mkString("'", "','", "'")
      val insertQueryUpdated = insertSql(idsClause)
      log.info(s"Executing: $insertQueryUpdated")
      insertedRecordsCount = sql.update(insertQueryUpdated)
      val deleteSql = s"delete from ${schema}_stage.staging_${factEntity}_session  where ${factEntity}_session_staging_id in (${deleteIds.mkString("'", "','", "'")})"
      log.info(s"Executing: $deleteSql")
      deletedRecordsCount = sql.update(deleteSql)
    }
    log.info(s"Completed ${factEntity} events put in ${factEntity}_fact and deleted from staging_fact")
    log.info(s"Records affected count: inserted=${insertedRecordsCount}, deleted=${deletedRecordsCount}")
    log.info(s"It took: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
  }

  def processDanglingEvents(deleteDanglingSql: String): Unit = {
    if (deleteDanglingSql != null) {
      log.info("Starting Transaction")
      val startTime = System.currentTimeMillis()
      var recordsAffectedCount = 0
      log.info(s"Executing: $deleteDanglingSql")
      sql localTx { implicit session =>
        recordsAffectedCount = sql.update(deleteDanglingSql)
      }
      log.info(s"Dangling started ${factEntity} events deleted from staging_fact")
      log.info(s"Records affected count: ${recordsAffectedCount}")
      log.info(s"It took: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    }
  }
}