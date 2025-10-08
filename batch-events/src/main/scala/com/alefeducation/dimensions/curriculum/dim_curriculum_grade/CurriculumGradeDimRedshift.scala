package com.alefeducation.dimensions.curriculum.dim_curriculum_grade

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CurriculumGradeCreatedEvent
import com.alefeducation.util.Helpers.RedshiftCurriculumGradeCreatedSink
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.SparkSessionUtils

object CurriculumGradeDimRedshift {

  private val currGradeCreatedRedshiftService = "redshift-curriculum-grade-created-service"

  private val session = SparkSessionUtils.getSession(currGradeCreatedRedshiftService)
  val service = new SparkBatchService(currGradeCreatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currGradeCreatedTransformedDF = service.readOptional(getSource(currGradeCreatedRedshiftService).head, session)

      currGradeCreatedTransformedDF.map(
        _.toRedshiftInsertSink(
          RedshiftCurriculumGradeCreatedSink,
          CurriculumGradeCreatedEvent
        )
      )
    }
  }
}