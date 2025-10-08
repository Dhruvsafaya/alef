package com.alefeducation.util

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.Resources.{getList, getSink, getSource}
import org.apache.spark.sql.SparkSession

import scala.util.Try

class DeltaBatchWriter(val session: SparkSession,
                       val service: SparkBatchService,
                       val sources: List[String],
                       val sink: String,
                       val isFact: Boolean) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def write(): List[Option[Sink]] = {
    sources.map(source => {
      val df = service.readOptional(source, session)
      df.flatMap(_.toCreate(isFact = isFact).map(_.toSink(sink)))
    })
  }
  
}

object DeltaBatchWriter {

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val source = Try(getSource(serviceName)).getOrElse(List.empty)
    val sink = getSink(serviceName).head
    val sources = getList(serviceName, "sources")
    val combinedSources = sources ++ source

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val isFact = args(1).toBoolean

    val writer = new DeltaBatchWriter(session, service, combinedSources, sink, isFact)
    service.runAll(writer.write().flatten)
  }
}
