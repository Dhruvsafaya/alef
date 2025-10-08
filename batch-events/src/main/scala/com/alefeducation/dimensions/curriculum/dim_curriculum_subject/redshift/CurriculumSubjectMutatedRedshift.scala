package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{CurriculumSubjectDeletedEvent, CurriculumSubjectUpdatedEvent}
import com.alefeducation.util.Helpers.{CurriculumSubjectEntity, RedshiftCurriculumSubjectSink}
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.SparkSessionUtils

object CurriculumSubjectMutatedRedshift {

  private val currSubjectMutatedRedshiftService: String = "redshift-curriculum-subject-mutated-service"
  private val currSubjectSources = getSource(currSubjectMutatedRedshiftService)
  private val currSubjectUpdatedRedshiftSrc: String = currSubjectSources(0)
  private val currSubjectDeletedRedshiftSrc: String = currSubjectSources(1)

  private val session = SparkSessionUtils.getSession(currSubjectMutatedRedshiftService)
  val service = new SparkBatchService(currSubjectMutatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currSubjectUpdatedTransformedDF = service.readOptional(currSubjectUpdatedRedshiftSrc, session)

      currSubjectUpdatedTransformedDF.map(
        _.toRedshiftUpdateSink(RedshiftCurriculumSubjectSink, CurriculumSubjectUpdatedEvent, CurriculumSubjectEntity)
      )
    }

    service.run {
      val currSubjectDeletedTransformedDF = service.readOptional(currSubjectDeletedRedshiftSrc, session)

      currSubjectDeletedTransformedDF.map(
        _.toRedshiftUpdateSink(RedshiftCurriculumSubjectSink, CurriculumSubjectDeletedEvent, CurriculumSubjectEntity)
      )
    }
  }
}