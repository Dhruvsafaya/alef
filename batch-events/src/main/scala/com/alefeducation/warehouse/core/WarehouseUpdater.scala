package com.alefeducation.warehouse.core

import com.alefeducation.warehouse.models.WarehouseConnection
import scalikejdbc.{AutoSession, DB, SQL}

trait WarehouseUpdater extends Warehouse[String] {

  override def runQueries(connection: WarehouseConnection, queries: List[String])(implicit session: AutoSession): Unit = {
    queries.foreach { query =>
      DB localTx { implicit session =>
        SQL(query).update.apply
        log.info(s"Updater execute query: $query")
      }
    }
  }
}
