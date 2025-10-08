package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform.CurriculumSubjectCreatedTransform.{CurriculumSubjectCreatedSinkName, CurriculumSubjectCreatedSourceName, CurriculumSubjectDimKey}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CurriculumSubjectCreatedTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): List[Option[Sink]] = {

    val currSubjectCreatedDF: Option[DataFrame] = service.readOptional(CurriculumSubjectCreatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val currSubjectCreatedTransformedDF = currSubjectCreatedDF.map(
      _.transformForInsertDim(CurriculumSbjMutatedDimCols , CurriculumSubjectEntity, List("subjectId"))
    )

    List(currSubjectCreatedTransformedDF.map(DataSink(CurriculumSubjectCreatedSinkName, _)))
  }

}

object CurriculumSubjectCreatedTransform {

  private val CurriculumSubjectCreatedTransformService: String = "transform-curriculum-subject-created-service"

  val CurriculumSubjectDimKey = "dim_curriculum_subject"

  val CurriculumSubjectCreatedSourceName: String = getSource(CurriculumSubjectCreatedTransformService).head
  val CurriculumSubjectCreatedSinkName: String = getSink(CurriculumSubjectCreatedTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(CurriculumSubjectCreatedTransformService)
  val service = new SparkBatchService(CurriculumSubjectCreatedTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new CurriculumSubjectCreatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
