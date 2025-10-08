package com.alefeducation.dimensions.course_activity_container.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_container.redshift.ContainerMutatedRedshift.{prefix, sinkConf, sourceConf}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class ContainerMutatedRedshift(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    val adt = service.readOptional(sourceConf, session)
    val adtSink = adt.map(
      _.drop("course_activity_container_description")
        .drop("course_activity_container_metadata")
        .toRedshiftIWHSink(
          sinkConf,
          prefix,
          ids = List("course_activity_container_id"),
          isStagingSink = true
        )
    )

    List(adtSink)
  }
}

object ContainerMutatedRedshift {

  val serviceConf = "redshift-course-activity-container"
  val sourceConf: String = "parquet-container-mutated-transformed-source"
  val sinkConf: String = "redshift-course-activity-container-sink"
  val prefix: String = "course_activity_container"

  private val session = SparkSessionUtils.getSession(serviceConf)
  val service = new SparkBatchService(serviceConf, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ContainerMutatedRedshift(session, service)
    service.runAll(transformer.transform().flatten)
  }

}