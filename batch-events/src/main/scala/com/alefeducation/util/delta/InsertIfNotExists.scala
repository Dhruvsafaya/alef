package com.alefeducation.util.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.util.Resources.{getList, getNestedString, getNestedStringIfPresent, getOptionString, getSink, getSource, getUniqueIds}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class InsertIfNotExists(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def write(): Option[Sink] = {
    val source = getSource(serviceName).head
    val sink = getSink(serviceName).head
    val entity = getOptionString(serviceName, "entity-prefix").getOrElse("")
    val matchCondition = getNestedStringIfPresent(serviceName, "match-conditions")
      .getOrElse(getUniqueIds(serviceName).map(id => s"${Alias.Delta}.$id = ${Alias.Events}.$id").mkString(" and "))
    val filterNot = getList(serviceName, "exclude-columns")

    val sourceDf = service.readOptional(source, session)

    sourceDf.map(_.toInsertIfNotExists(matchCondition, filterNot)).map(_.toSink(sink, entity))
  }
}


object InsertIfNotExists {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new InsertIfNotExists(session, service, serviceName)
    service.run(writer.write())
  }
}
