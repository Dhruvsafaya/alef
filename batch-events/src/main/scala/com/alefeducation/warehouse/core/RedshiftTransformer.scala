package com.alefeducation.warehouse.core

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.models.QueryMeta
import scalikejdbc.{AutoSession, DB, SQL}

import scala.annotation.tailrec

@deprecated("extend CommonTransformer interface for impl. Transformer")
trait RedshiftTransformer extends WarehouseTransformer {

  def isUser(data: String): Boolean = Set("user", "teacher", "student", "guardian").contains(data)
  def isSCDDim(data: String): Boolean = Set("content_repository", "class", "question").contains(data)

  override def runQueries(connection: WarehouseConnection, queryMeta: List[QueryMeta])(implicit session: AutoSession): Unit = {
    def fetchStagingIds(meta: QueryMeta) = SQL(meta.selectSQL).map(_.string(getPkColumn(meta.stagingTable))).list.apply

    @tailrec
    def populateWarehouse(meta: QueryMeta, pkCol: String, ids: List[String]): Unit = {
      if (ids.nonEmpty) {
        log.info(s"Query to be run for ${meta.stagingTable} has : ${ids.size} records")
        val startTime = System.currentTimeMillis()
        val deleteQuery =
          s"delete from ${connection.schema}_stage.${meta.stagingTable} where $pkCol in (${ids.map(x => "'" + x + "'").mkString(",")})"

        DB localTx { implicit session =>
          SQL(meta.insertSQL).update.apply
          SQL(deleteQuery).update.apply
        }

        log.info(s"Insert and Delete for ${ids.size} records took: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
        populateWarehouse(meta, pkCol, fetchStagingIds(meta))
      } else log.info(s"Query skipped for ${meta.stagingTable} as it has : ${ids.size} records")

    }

    queryMeta.foreach { meta =>
      val pkCol = getPkColumn(meta.stagingTable)
      val ids = fetchStagingIds(meta)
      populateWarehouse(meta, pkCol, ids)
    }
  }

  def stripDwColumn(col: String, prefix: String = ""): String = col.stripPrefix(prefix + "_").stripSuffix("_dw_id")

}
