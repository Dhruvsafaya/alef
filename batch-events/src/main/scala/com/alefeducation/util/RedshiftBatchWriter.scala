package com.alefeducation.util

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.util.Try

class RedshiftBatchWriter(val session: SparkSession, val service: SparkBatchService, val sources: List[String], val sink: String) {
  def write(): List[Option[Sink]] = {
    sources.map(source => {
      val df = service.readOptional(source, session)
      df.map(_.toRedshiftInsertSink(sink))
    })
  }
}

object RedshiftBatchWriter {

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)

    val source = Try(getSource(serviceName)).getOrElse(List.empty)

    val sink = getSink(serviceName).head
    val sources = getList(serviceName, "sources")
    val combinedSources: List[String] = sources ++ source

    val session = SparkSessionUtils.getSession(serviceName)

    val service = new SparkBatchService(serviceName, session)

    val writer = new RedshiftBatchWriter(session, service, combinedSources, sink)
    service.runAll(writer.write().flatten)
  }
}
