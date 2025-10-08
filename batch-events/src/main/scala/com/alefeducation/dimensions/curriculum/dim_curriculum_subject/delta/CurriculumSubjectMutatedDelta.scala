package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.Helpers.{CurriculumSubjectEntity, DeltaCurriculumSubjectMutatedSink}
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.SparkSessionUtils

object CurriculumSubjectMutatedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val currSubjectMutatedService = "delta-curriculum-subject-mutated-service"
  private val currSubjectSources = getSource(currSubjectMutatedService)
  private val currSubjectUpdatedRedshiftSrc: String = currSubjectSources(0)
  private val currSubjectDeletedRedshiftSrc: String = currSubjectSources(1)

  private val session = SparkSessionUtils.getSession(currSubjectMutatedService)
  val service = new SparkBatchService(currSubjectMutatedService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currSubjectUpdatedTransformedDF = service.readOptional(currSubjectUpdatedRedshiftSrc, session)

      currSubjectUpdatedTransformedDF.flatMap(_.toUpdate().map(_.toSink(DeltaCurriculumSubjectMutatedSink, CurriculumSubjectEntity)))
    }

    service.run {
      val currSubjectDeletedTransformedDF = service.readOptional(currSubjectDeletedRedshiftSrc, session)

      currSubjectDeletedTransformedDF.flatMap(_.toDelete().map(_.toSink(DeltaCurriculumSubjectMutatedSink, CurriculumSubjectEntity)))
    }
  }
}
