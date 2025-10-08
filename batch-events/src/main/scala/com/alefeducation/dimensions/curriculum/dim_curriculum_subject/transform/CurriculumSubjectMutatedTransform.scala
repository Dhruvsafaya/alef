package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform.CurriculumSubjectMutatedTransform.{CurriculumSubjectDeletedSinkName, CurriculumSubjectDeletedSourceName, CurriculumSubjectMutatedSinkName, CurriculumSubjectUpdatedSourceName}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class CurriculumSubjectMutatedTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): List[Option[Sink]] = {

    val currSubjectUpdatedDF: Option[DataFrame] = service.readOptional(CurriculumSubjectUpdatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val currSubjectUpdatedTransformedDF = currSubjectUpdatedDF.map(_.transformForUpdateDim(CurriculumSbjMutatedDimCols, CurriculumSubjectEntity, List("subjectId")))

    val currSubjectDeletedDF: Option[DataFrame] = service.readOptional(CurriculumSubjectDeletedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val currSubjectDeletedTransformedDF = currSubjectDeletedDF.map(_.transformForDelete(CurriculumSbjDeletedDimCols, CurriculumSubjectEntity, List("subjectId")))

    List(
      currSubjectUpdatedTransformedDF.map(DataSink(CurriculumSubjectMutatedSinkName, _)),
      currSubjectDeletedTransformedDF.map(DataSink(CurriculumSubjectDeletedSinkName, _))
    )
  }

}

object CurriculumSubjectMutatedTransform {

  private val CurriculumSubjectMutatedTransformService: String = "transform-curriculum-subject-mutated-service"

  private val CurriculumSubjectMutatedSources = getSource(CurriculumSubjectMutatedTransformService)
  private val CurriculumSubjectMutatedSinks = getSink(CurriculumSubjectMutatedTransformService)

  val CurriculumSubjectUpdatedSourceName: String = CurriculumSubjectMutatedSources.head
  val CurriculumSubjectMutatedSinkName: String = CurriculumSubjectMutatedSinks.head

  val CurriculumSubjectDeletedSourceName: String = CurriculumSubjectMutatedSources(1)
  val CurriculumSubjectDeletedSinkName: String = CurriculumSubjectMutatedSinks(1)


  val session: SparkSession = SparkSessionUtils.getSession(CurriculumSubjectMutatedTransformService)
  val service = new SparkBatchService(CurriculumSubjectMutatedTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new CurriculumSubjectMutatedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
