package com.alefeducation.util.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.util.Resources.{getNestedString, getOptionString, getSink, getUniqueIds}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class DeltaSCDTypeII(val session: SparkSession,
                     val service: SparkBatchService,
                     val serviceName: String) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
  def write(): Option[Sink] = {
    val source = getNestedString(serviceName, "source")
    val sink = getSink(serviceName).head
    val entity = getOptionString(serviceName, "entity-prefix").getOrElse("")
    val uniqueIds = getUniqueIds(serviceName)
    val matchCondition = uniqueIds.map(id => s"delta.$id = ${Alias.Events}.$id").mkString(" and ")

    val df = service.readOptional(source, session)

    df.map(_.toSCDTypeII(matchCondition, uniqueIds)).map(_.toSink(sink, entity))
  }
}

object DeltaSCDTypeII {
  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new DeltaSCDTypeII(session, service, serviceName)
    service.run(writer.write())
  }
}
