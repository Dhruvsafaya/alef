package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

@deprecated("Please use GenericSCD1Transform instead")
class GenericDimUpdatedTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String)  {
  def transform(): Option[Sink] = {
    import com.alefeducation.util.BatchTransformerUtility._

    service.readOptional(getList(serviceName, "updated-source").head, session,
      extraProps = List(("mergeSchema", "true"))).map(_.transformForInsertOrUpdate(
      getNestedMap(serviceName, "column-mapping"),
      getNestedString(serviceName, "entity"),
      getList(serviceName, "unique-ids")
    )).map(DataSink(getList(serviceName, "updated-sink").head, _))
  }
}

@deprecated("Please use GenericSCD1Transform instead")
object GenericDimUpdatedTransform {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericDimUpdatedTransform(session, service, args(0))
    service.run(transformer.transform())
  }
}
