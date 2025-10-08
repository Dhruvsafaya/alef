package com.alefeducation.listener

import com.paulgoldbaum.influxdbclient.Parameter.Precision
import com.paulgoldbaum.influxdbclient.{Database, InfluxDB, Point}
import org.apache.log4j.Logger
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import scala.collection.concurrent.TrieMap
import scala.collection.concurrent.Map

object Holder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

class SinkMetricsWriter(val sinkName: String,
                        val env: String,
                        val host: String,
                        val port: Int,
                        val user: String,
                        val password: String,
                        val https: Boolean,
                        val database: String) extends ForeachWriter[Row]{

  var influxDB: Option[InfluxDB] = None
  var influxDatabase: Option[Database] = None

  val accumulator: Map[String, Long] = TrieMap.empty[String, Long]

  override def open(partitionId: Long, version: Long): Boolean = {
    influxDB = Option(InfluxDB.connect(host, port, user, password, https))
    influxDatabase = influxDB.map(_.selectDatabase(database))
    true
  }

  override def process(row: Row): Unit = {
    val tenantId = Try(row.getAs[String]("tenantId")).getOrElse("UNDEFINED_TENANT_ID")

    var oldValue: Long = 0
    var newValue: Long = 0
    do {
      oldValue = accumulator.getOrElseUpdate(tenantId, 0)
      newValue = oldValue + 1
    } while (!accumulator.replace(tenantId, oldValue, newValue))
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (errorOrNull != null) {

      influxDatabase.foreach(_.write(
        Point(s"exceptions_$env", System.currentTimeMillis()).addField("count", 1).addTag("sink", sinkName),
        precision = Precision.MILLISECONDS
      ).andThen {
        case Success(_) =>
          Holder.log.debug(s"Metrics reported to InfluxDB")
        case Failure(e) =>
          Holder.log.error("Error reported exceptions metrics to InfluxDB", e)
      })
    }

    val points = accumulator.toList.map {
      case (tenantId, value) => Point(s"sinks_$env", System.currentTimeMillis())
                                  .addField("num_processed_row", value)
                                  .addField("tenant_id", tenantId)
                                  .addTag("sink", sinkName)
    }
    accumulator.clear()

    if (points.nonEmpty) {
      influxDatabase.foreach(_.bulkWrite(points, precision = Precision.MILLISECONDS).andThen {
        case Success(_) =>
          Holder.log.debug(s"Metrics reported to InfluxDB")
        case Failure(e) =>
          Holder.log.error("Error reported sinks metrics to InfluxDB", e)
      })
    }
  }
}