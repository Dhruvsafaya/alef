package com.alefeducation.dimensions.curriculum.dim_curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{CurriculumCreatedEvent}
import com.alefeducation.util.Helpers.RedshiftCurriculumCreatedSink
import com.alefeducation.util.Resources.getSource
import com.alefeducation.util.SparkSessionUtils

object CurriculumDimRedshift {

  private val currCreatedRedshiftService = "redshift-curriculum-created-service"

  private val session = SparkSessionUtils.getSession(currCreatedRedshiftService)
  val service = new SparkBatchService(currCreatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val currCreatedTransformedDF = service.readOptional(getSource(currCreatedRedshiftService).head, session)

      currCreatedTransformedDF.map(
        _.toRedshiftInsertSink(
          RedshiftCurriculumCreatedSink,
          CurriculumCreatedEvent
        )
      )
    }
  }
}