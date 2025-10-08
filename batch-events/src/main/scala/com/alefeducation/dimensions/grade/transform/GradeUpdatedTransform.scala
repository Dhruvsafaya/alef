package com.alefeducation.dimensions.grade.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.gradeUniqueIds
import com.alefeducation.dimensions.grade.transform.GradeUpdatedTransform.{GradeUpdatedSinkName, GradeUpdatedSourceName}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class GradeUpdatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val gradeUpdatedDf: Option[DataFrame] = service.readOptional(GradeUpdatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val updatedTransformedDf = gradeUpdatedDf.map(_.filter(col(EventType) === AdminGradeUpdated)
      .transformForUpdateDim(GradeDimensionCols, GradeEntity, gradeUniqueIds))

    List(
      updatedTransformedDf.map(DataSink(GradeUpdatedSinkName, _))
    )

  }

}

object GradeUpdatedTransform {

  private val GradeUpdatedTransformService: String = "transform-grade-updated-service"

  val GradeUpdatedSourceName: String = getSource(GradeUpdatedTransformService).head
  val GradeUpdatedSinkName: String = getSink(GradeUpdatedTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(GradeUpdatedTransformService)
  val service = new SparkBatchService(GradeUpdatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GradeUpdatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
