package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

@deprecated("Please use GenericSCD1Transform instead")
class GenericDimCreatedTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {
    def transform(): Option[Sink] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val startId = service.getStartIdUpdateStatus(getNestedString(serviceName, "key"))

    service.readOptional(getList(serviceName, "created-source").head, session,
      extraProps = List(("mergeSchema", "true"))).map(_.transformForInsertOrUpdate(
      getNestedMap(serviceName, "column-mapping"),
      getNestedString(serviceName, "entity"),
      getList(serviceName, "unique-ids")
    ).genDwId(s"${getNestedString(serviceName, "entity")}_dw_id", startId))
      .map(DataSink(getList(serviceName, "created-sink").head, _, controlTableUpdateOptions = Map(ProductMaxIdType -> getNestedString(serviceName, "key"))))
  }
}

@deprecated("Please use GenericSCD1Transform instead")
object GenericDimCreatedTransform {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericDimCreatedTransform(session, service, args(0))
    service.run(transformer.transform())
  }
}