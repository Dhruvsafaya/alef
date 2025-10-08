package com.alefeducation.dimensions.curriculum.dim_curriculum_subject.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CurriculumSubjectCreatedEvent
import com.alefeducation.util.Helpers.RedshiftCurriculumSubjectSink
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.SparkSessionUtils

object CurriculumSubjectCreatedRedshift {

  private val currSubjectCreatedRedshiftService = "redshift-curriculum-subject-created-service"

  private val session = SparkSessionUtils.getSession(currSubjectCreatedRedshiftService)
  val service = new SparkBatchService(currSubjectCreatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currSubjectCreatedTransformedDF = service.readOptional(getSource(currSubjectCreatedRedshiftService).head, session)

      currSubjectCreatedTransformedDF.map(
        _.toRedshiftInsertSink(RedshiftCurriculumSubjectSink, CurriculumSubjectCreatedEvent)
      )

    }
  }
}