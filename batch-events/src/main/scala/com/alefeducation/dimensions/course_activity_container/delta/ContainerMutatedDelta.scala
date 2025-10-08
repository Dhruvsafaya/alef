package com.alefeducation.dimensions.course_activity_container.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.course_activity_container.delta.ContainerMutatedDelta.{deltaMatchCondition, prefix, sinkConf, sourceConf}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession


class ContainerMutatedDelta(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def transform(): List[Option[Sink]] = {

    val source = service.readOptional(sourceConf, session)
    val sink = source.flatMap(
      _.toIWH(
        matchConditions = deltaMatchCondition,
        uniqueIdColumns = List("course_activity_container_id"),

      )
        .map(_.toSink(sinkConf, prefix))
    )

    List(sink)
  }
}

object ContainerMutatedDelta {

  val serviceConf = "delta-course-activity-container"
  val sourceConf: String = "parquet-container-mutated-transformed-source"
  val sinkConf: String = "delta-course-activity-container-sink"
  val prefix: String = "course_activity_container"

  private val deltaMatchCondition =
    s"""
       |${Alias.Delta}.course_activity_container_id = ${Alias.Events}.course_activity_container_id
     """.stripMargin

  private val session = SparkSessionUtils.getSession(serviceConf)
  val service = new SparkBatchService(serviceConf, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ContainerMutatedDelta(session, service)
    service.runAll(transformer.transform().flatten)
  }

}

