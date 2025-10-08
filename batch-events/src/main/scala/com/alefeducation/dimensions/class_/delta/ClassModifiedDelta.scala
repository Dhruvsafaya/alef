package com.alefeducation.dimensions.class_.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.dimensions.class_.delta.ClassModifiedDelta.{classModifiedService, classModifiedSession, classModifiedSinkName, classModifiedSourceName}
import com.alefeducation.dimensions.class_.transform.ClassModifiedTransform.classModifiedTransformSink
import com.alefeducation.util.Helpers.{ClassEntity, DeltaClassSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class ClassModifiedDelta(val session: SparkSession, val service: SparkBatchService) {

  import Delta.Transformations._
  def transform(): Option[Sink] = {
    val classModified = service.readOptional(classModifiedSourceName, session)
    classModified.map(_.toSCDContext(uniqueIdColumns = List("class_id")).toSink(classModifiedSinkName, ClassEntity))
  }
}
object ClassModifiedDelta {
  val classModifiedSourceName: String = classModifiedTransformSink
  val classModifiedSinkName: String = DeltaClassSink
  private val classModifiedServiceName = "delta-class-modified"

  private val classModifiedSession: SparkSession = SparkSessionUtils.getSession(classModifiedServiceName)
  private val classModifiedService = new SparkBatchService(classModifiedServiceName, classModifiedSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassModifiedDelta(classModifiedSession, classModifiedService)
    classModifiedService.run(transformer.transform())
  }

}
