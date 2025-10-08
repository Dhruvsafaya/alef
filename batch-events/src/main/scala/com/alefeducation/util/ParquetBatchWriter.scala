package com.alefeducation.util

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.SparkSession

class ParquetBatchWriter(val session: SparkSession,
                         val service: SparkBatchService,
                         val source: String,
                         val sink: String
                               ) {
  def write(): Option[Sink] = {
    val df = service.readOptional(source, session)

    import com.alefeducation.util.BatchTransformerUtility._
    df.map(_.toParquetSink(sink))
  }
}

object ParquetBatchWriter {

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val source = getSource(serviceName).head
    val sink = getSink(serviceName).head

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new ParquetBatchWriter(session, service, source, sink)
    service.run(writer.write())
  }
}
