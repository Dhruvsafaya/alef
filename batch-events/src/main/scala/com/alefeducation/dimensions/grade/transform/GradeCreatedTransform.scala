package com.alefeducation.dimensions.grade.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.{GradeCreatedSinkName, GradeCreatedSourceName, gradeUniqueIds}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class GradeCreatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val gradeMutatedDf: Option[DataFrame] = service.readOptional(GradeCreatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val createdTransformedDf = gradeMutatedDf.map(
      _.filter(col(EventType) === AdminGradeCreated)
        .transformForInsertOrUpdate(GradeDimensionCols, GradeEntity, gradeUniqueIds)
    )

    List(
      createdTransformedDf.map(DataSink(GradeCreatedSinkName, _))
    )

  }

}

object GradeCreatedTransform {

  private val GradeCreatedTransformService: String = "transform-grade-created-service"

  val GradeCreatedSourceName: String = getSource(GradeCreatedTransformService).head
  val GradeCreatedSinkName: String = getSink(GradeCreatedTransformService).head

  val gradeUniqueIds: List[String] = List("uuid")

  val session: SparkSession = SparkSessionUtils.getSession(GradeCreatedTransformService)
  val service = new SparkBatchService(GradeCreatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GradeCreatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
