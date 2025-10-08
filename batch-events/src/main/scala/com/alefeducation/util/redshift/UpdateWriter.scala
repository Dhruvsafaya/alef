package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getBool, getList, getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession


class UpdateWriter(val session: SparkSession,
                   val service: SparkBatchService,
                   val serviceName: String) {
  import com.alefeducation.util.BatchTransformerUtility._

  def write(): Option[DataSink] = {

    val source = getSource(serviceName).head
    val sink = getSink(serviceName).head

    val sourceDf = service.readOptional(source, session)

    sourceDf.map(
      _.toRedshiftUpdateSink(
        sinkName = sink,
        eventType = getNestedString(serviceName, "event-name"),
        entity = getNestedString(serviceName, "entity-prefix"),
        ids = getList(serviceName, "unique-ids"),
        isStaging = getBool(serviceName, "is-staging")
      ))
  }

}

object UpdateWriter {
  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new UpdateWriter(session, service, serviceName)
    service.run(writer.write())
  }
}
