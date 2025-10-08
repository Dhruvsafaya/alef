package com.alefeducation.dimensions.class_.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.redshift.ClassModifiedRedshift.{modifiedSinkName, modifiedSourceName}
import com.alefeducation.dimensions.class_.transform.ClassModifiedTransform.classModifiedTransformSink
import com.alefeducation.util.Constants.ClassUpdatedEvent
import com.alefeducation.util.DataFrameUtility.setScdTypeIIPostAction
import com.alefeducation.util.Helpers.{ClassEntity, RedshiftClassStageSink}
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

class ClassModifiedRedshift(val session: SparkSession, val service: SparkBatchService) {

  import BatchTransformerUtility._
  def transform(): Option[Sink] = {
    val classModified = service.readOptional(modifiedSourceName, session)
    classModified.map(_.toRedshiftSCDSink(modifiedSinkName, ClassUpdatedEvent, ClassEntity, setScdTypeIIPostAction))
  }
}
object ClassModifiedRedshift {
  val modifiedSourceName: String = classModifiedTransformSink
  val modifiedSinkName: String = RedshiftClassStageSink
  private val modifiedServiceName: String = "redshift-class-modified"

  private val modifiedSession: SparkSession = SparkSessionUtils.getSession(modifiedServiceName)
  private val modifiedService = new SparkBatchService(modifiedServiceName, modifiedSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassModifiedRedshift(modifiedSession, modifiedService)
    modifiedService.run(transformer.transform())
  }

}
