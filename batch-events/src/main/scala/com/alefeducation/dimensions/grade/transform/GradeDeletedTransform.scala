package com.alefeducation.dimensions.grade.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.gradeUniqueIds
import com.alefeducation.dimensions.grade.transform.GradeDeletedTransform.{GradeDeletedSinkName, GradeDeletedSourceName}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class GradeDeletedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val gradeDeletedDf: Option[DataFrame] = service.readOptional(GradeDeletedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val deletedTransformedDf = gradeDeletedDf.map(_.transformForDelete(GradeDeletedDimensionCols, GradeEntity, gradeUniqueIds))

    List(
      deletedTransformedDf.map(DataSink(GradeDeletedSinkName, _))
    )

  }

}

object GradeDeletedTransform {

  private val GradeDeletedTransformService: String = "transform-grade-deleted-service"

  val GradeDeletedSourceName: String = getSource(GradeDeletedTransformService).head

  val GradeDeletedSinkName: String = getSink(GradeDeletedTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(GradeDeletedTransformService)
  val service = new SparkBatchService(GradeDeletedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GradeDeletedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
