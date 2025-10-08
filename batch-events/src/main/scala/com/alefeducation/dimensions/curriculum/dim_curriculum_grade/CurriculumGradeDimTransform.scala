package com.alefeducation.dimensions.curriculum.dim_curriculum_grade

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.curriculum.dim_curriculum_grade.CurriculumGradeDimTransform.{CurriculumGradeDimKey, CurriculumGradeDimSinkName, CurriculumGradeSourceName}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CurriculumGradeDimTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): List[Option[Sink]] = {

    val currGradeCreatedDF: Option[DataFrame] = service.readOptional(CurriculumGradeSourceName, session, extraProps = List(("mergeSchema", "true")))

    val currGradeCreatedTransformedDF = currGradeCreatedDF.map(
      _.transformForInsertDim(CurriculumGradeCreatedDimCols, CurriculumGradeEntity, List("gradeId"))
    )

    List(currGradeCreatedTransformedDF.map(DataSink(CurriculumGradeDimSinkName, _)))
  }

}

object CurriculumGradeDimTransform {

  private val CurriculumGradeTransformService: String = "transform-curriculum-grade-created-service"

  val CurriculumGradeDimKey = "dim_curriculum_grade"

  val CurriculumGradeSourceName: String = getSource(CurriculumGradeTransformService).head
  val CurriculumGradeDimSinkName: String = getSink(CurriculumGradeTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(CurriculumGradeTransformService)
  val service = new SparkBatchService(CurriculumGradeTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new CurriculumGradeDimTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
