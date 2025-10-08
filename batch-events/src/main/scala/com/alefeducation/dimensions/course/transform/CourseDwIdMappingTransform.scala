package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course.transform.CourseDwIdMappingTransform.CourseDwIdMappingSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._


class CourseDwIdMappingTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {

    val courseDraftCreated: Option[DataFrame] = service.readOptional(CourseDwIdMappingTransform.CoursePublishedParquetSource, session)

    val dwIdMappingDf = courseDraftCreated.flatMap(
      _.select("id", "occurredOn")
        .transformForInsertDwIdMapping("entity", "course")
        .checkEmptyDf
    )

    List(
      dwIdMappingDf.map(DataSink(CourseDwIdMappingSink, _)),
    )
  }
}

object CourseDwIdMappingTransform {
  val CourseDwIdMappingTransformService = "course-dw-id-mapping-transform"
  val CoursePublishedParquetSource = "parquet-course-published-source"
  val CourseDwIdMappingSink = "parquet-course-dw-id-mapping-transformed-source"

  val session: SparkSession = SparkSessionUtils.getSession(CourseDwIdMappingTransformService)
  val service = new SparkBatchService(CourseDwIdMappingTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new CourseDwIdMappingTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
